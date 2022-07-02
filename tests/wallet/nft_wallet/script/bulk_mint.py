import argparse
import asyncio
import csv
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from blspy import AugSchemeMPL, G1Element, G2Element, PrivateKey
from clvm.casts import int_from_bytes, int_to_bytes

from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.rpc.rpc_client import RpcClient
from chia.rpc.wallet_rpc_client import WalletRpcClient
from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.spend_bundle import SpendBundle
from chia.util.bech32m import decode_puzzle_hash, encode_puzzle_hash
from chia.util.byte_types import hexstr_to_bytes
from chia.util.condition_tools import ConditionOpcode
from chia.util.config import load_config
from chia.util.default_root import DEFAULT_ROOT_PATH
from chia.util.ints import uint16, uint64
from chia.wallet.puzzles.load_clvm import load_clvm
from chia.wallet.util.wallet_types import WalletType

# --------------------------------
# CONSTANTS
# --------------------------------

config = load_config(Path(DEFAULT_ROOT_PATH), "config.yaml")
testnet_agg_sig_data = config["network_overrides"]["constants"]["testnet10"]["AGG_SIG_ME_ADDITIONAL_DATA"]
DEFAULT_CONSTANTS = DEFAULT_CONSTANTS.replace_str_to_bytes(**{"AGG_SIG_ME_ADDITIONAL_DATA": testnet_agg_sig_data})

csv_filename = "sample.csv"
target_filename = "target_sample.csv"
chunk = 25
royalty_address = "did:chia:1t0nmt3cjeclutcyunmpwq9l4wak99g4wegs2f3kxu6vzwrhdeg7qkw9ne7"
royalty_percentage = 300
fingerprint = 1455603606
min_fee = 1


class NFTManager:
    def __init__(
        self,
        wallet_client: WalletRpcClient = None,
        node_client: FullNodeRpcClient = None,
    ) -> None:
        self.wallet_client = wallet_client
        self.node_client = node_client

    async def connect(self, wallet_index: int = 0) -> None:
        config = load_config(Path(DEFAULT_ROOT_PATH), "config.yaml")
        rpc_host = config["self_hostname"]
        full_node_rpc_port = config["full_node"]["rpc_port"]
        wallet_rpc_port = config["wallet"]["rpc_port"]
        if not self.node_client:
            self.node_client = await FullNodeRpcClient.create(
                rpc_host, uint16(full_node_rpc_port), Path(DEFAULT_ROOT_PATH), config
            )
        if not self.wallet_client:
            self.wallet_client = await WalletRpcClient.create(
                rpc_host, uint16(wallet_rpc_port), Path(DEFAULT_ROOT_PATH), config
            )
        await self.wallet_client.log_in(fingerprint)
        main_wallets = await self.wallet_client.get_wallets(wallet_type=WalletType.STANDARD_WALLET)
        did_wallets = await self.wallet_client.get_wallets(wallet_type=WalletType.DECENTRALIZED_ID)
        nft_wallets = await self.wallet_client.get_wallets(wallet_type=WalletType.NFT)
        self.main_wallet_id = main_wallets[0]["id"]
        self.did_wallet_id = did_wallets[0]["id"]
        self.nft_wallet_id = nft_wallets[0]["id"]
        sync = await self.check_wallet_sync()
        if not sync:
            raise ValueError("Wallet is not synced. Resync and try again")
            

    async def close(self) -> None:
        if self.node_client:
            self.node_client.close()

        if self.wallet_client:
            self.wallet_client.close()

    async def current_did_id(self):
        did_info = await self.wallet_client.get_did_id(self.did_wallet_id)
        return did_info["my_did"]

    async def get_new_nfts(self, sb):
        did_info = await self.wallet_client.get_did_id(self.did_wallet_id)
        did_coin_id = bytes32(hexstr_to_bytes(did_info["coin_id"]))
        new_nfts = [
            "0x" + coin.name().hex()
            for coin in sb.additions()
            if coin.parent_coin_info != did_coin_id and coin.amount == 1
        ]
        return new_nfts

    async def mint_nfts(self, data, royalty_address, royalty_percentage, fee):
        did_id = await self.current_did_id()
        metadata_list = []
        for row in data:
            metadata = {
                "hash": row[0],
                "uris": [row[1]],
                "meta_hash": row[2],
                "meta_uris": [row[3]],
                "license_hash": row[4],
                "license_uris": [row[5]],
                "series_number": row[6],
                "series_total": row[7],
            }
            metadata_list.append(metadata)
        resp = await self.wallet_client.bulk_mint_nft(
            wallet_id=self.nft_wallet_id,
            metadata_list=metadata_list,
            royalty_address=royalty_address,
            royalty_percentage=royalty_percentage,
            did_id=did_id,
            fee=fee,
        )
        if not resp["success"]:
            logging.error("Minting failed for rows %s to %s: %s" % (start_row, end_row, resp))
            raise ValueError("Minting failed for rows %s to %s" % (start_row, end_row))

        return resp

    async def set_did(self, nft_id_list, fee):
        did_id = await self.current_did_id()
        resp = await self.wallet_client.bulk_set_nft_did(self.nft_wallet_id, did_id, nft_id_list, fee)
        if not resp["success"]:
            logging.error("Set DID failed for rows %s to %s: %s" % (start_row, end_row, resp))
            raise ValueError("Set DID failed for rows %s to %s" % (start_row, end_row))
        return resp

    async def transfer_nfts(self, nft_transfer_list, fee):
        resp = await self.wallet_client.bulk_nft_transfer(self.nft_wallet_id, nft_transfer_list, fee)
        if not resp["success"]:
            logging.error("Transfer failed for rows %s to %s: %s" % (start_row, end_row, resp))
            raise ValueError("Transfer failed for rows %s to %s" % (start_row, end_row))
        return resp

    async def wait_for_tx(self, tx_id):
        print("waiting for transaction: %s" % tx_id)
        seen = False
        while True:
            if seen:
                tx = await self.wallet_client.get_transaction(self.main_wallet_id, tx_id)
                if tx.confirmed:
                    break
                else:
                    await asyncio.sleep(10)
            else:
                seen = await self.check_mempool(tx_id)
                tx = await self.wallet_client.get_transaction(self.main_wallet_id, tx_id)
                if tx.confirmed:
                    break
                await asyncio.sleep(5)

    async def check_wallet_sync(self):
        synced = await self.wallet_client.get_synced()
        return synced

    async def check_mempool(self, tx_id=None):
        txs = await self.node_client.get_all_mempool_items()
        if tx_id in txs.keys():
            print("Transaction in Mempool")
            return True

    async def get_fee(self, spend_type):
        txs = await self.node_client.get_all_mempool_items()
        if len(txs) > 0:
            mempool_cost = sum([tx["cost"] for tx in txs.values()])
            fees = sorted([tx["fee"] for tx in txs.values()])
            pool_min_fee = fees[0]
        else:
            pool_min_fee = 0
            mempool_cost = 0
        if mempool_cost  >  5000000000:
            if spend_type == "mint":
                fee = 615000000 * chunk
            elif spend_type == "update":
                fee = 535000000 * chunk
            elif spend_type == "transfer":
                fee = 335000000 * chunk
        else:
            fee = pool_min_fee + min_fee
        return fee
            
# --------------------------------
# MANAGER FUNCTIONS FOR MINTING
# --------------------------------


async def mint(start_row, royalty_address, royalty_percentage) -> None:
    with open(csv_filename, "r") as f:
        csv_reader = csv.reader(f)
        bulk_data = list(csv_reader)
    manager = NFTManager()
    await manager.connect()
    fee = await manager.get_fee("mint")
    print("Minting Rows {} - {} | Fee = {}".format(start_row, start_row + chunk, fee))
    data = bulk_data[start_row : start_row + chunk]
    if start_row != 0:
        minted_data = get_minted()
        for row in data:
            if row in minted_data:
                raise ValueError("Already minted data for NFT {}\n Check start_row value is correct".format(row))

    mint_resp = await manager.mint_nfts(data, royalty_address, royalty_percentage, fee)
    mint_sb = SpendBundle.from_json_dict(mint_resp["spend_bundle"])
    new_nft_ids = await manager.get_new_nfts(mint_sb)
    set_nfts_to_update(new_nft_ids)
    mint_tx = bytes32(hexstr_to_bytes(mint_resp["tx_id"][2:]))
    set_minted(data)
    await manager.wait_for_tx(mint_tx)
    await manager.close()

    return new_nft_ids


async def update(new_nft_ids):
    manager = NFTManager()
    await manager.connect()
    fee = await manager.get_fee("update")
    start_row = get_state()
    print("Setting DID for Rows {} - {} |  Fee = {}".format(start_row, start_row + chunk, fee))
    nft_data = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nfts = nft_data["nft_list"]
    nft_ids = [nft["nft_coin_id"] for nft in nfts]
    coins_to_update = []
    for new_id in new_nft_ids:
        if new_id in nft_ids:
            coins_to_update.append(new_id)
    update_resp = await manager.set_did(coins_to_update, fee)
    update_sb = SpendBundle.from_json_dict(update_resp["spend_bundle"])
    updated_nft_ids = await manager.get_new_nfts(update_sb)
    update_tx = bytes32(hexstr_to_bytes(update_resp["tx_id"][2:]))
    await manager.wait_for_tx(update_tx)

    await manager.close()


async def transfer(start_row) -> None:
    with open(target_filename, "r") as f:
        target_data = f.read().splitlines()
    manager = NFTManager()
    await manager.connect()
    fee = await manager.get_fee("transfer")
    print("Transfering Rows {} - {} | Fee = {}".format(start_row, start_row + chunk, fee))
    data = target_data[start_row : start_row + chunk]
    if start_row != 0:
        targets_sent = get_targets_sent()
        for target in data:
            if target in targets_sent:
                raise ValueError("NFT already sent to address: {}".format(target))
    nfts = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nft_ids = [info["nft_coin_id"] for info in nfts["nft_list"]]
    assert len(nft_ids) > 0
    nft_transfer_list = list(zip(nft_ids, data))
    transfer_resp = await manager.transfer_nfts(nft_transfer_list, fee)
    transfer_sb = SpendBundle.from_json_dict(transfer_resp["spend_bundle"])
    transfer_tx = bytes32(hexstr_to_bytes(transfer_resp["tx_id"][2:]))
    await manager.wait_for_tx(transfer_tx)
    await manager.close()
    set_targets_sent(data)
    
# --------------------------------
# HELPER FUNCTIONS FOR DEVELOPMENT
# --------------------------------


async def view():
    manager = NFTManager()
    await manager.connect()

    nft_data = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nfts = nft_data["nft_list"]

    wallet_balance_res = await manager.wallet_client.get_wallet_balance(manager.main_wallet_id)
    wallet_balance = wallet_balance_res["confirmed_wallet_balance"]

    xch_coins = await manager.wallet_client.select_coins(amount=wallet_balance, wallet_id=manager.main_wallet_id)
    nft_list = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nft_coins = nft_data["nft_list"]
    breakpoint()

    await manager.close()


async def clear_nfts():
    manager = NFTManager()
    await manager.connect()
    nft_list = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nft_coins = nft_list["nft_list"]
    loops = int(len(nft_coins) / chunk)
    if loops == 0:
        loops = 1
    fee = 100
    start_row = 0
    for i in range(loops):
        await transfer(start_row, fee)
        start_row += chunk


async def split(num_coins, fee):
    manager = NFTManager()
    await manager.connect()

    wallet_balance_res = await manager.wallet_client.get_wallet_balance(manager.main_wallet_id)
    wallet_balance = wallet_balance_res["confirmed_wallet_balance"]

    xch_coins = await manager.wallet_client.select_coins(amount=wallet_balance, wallet_id=manager.main_wallet_id)

    final_coin_amt = uint64((wallet_balance - fee) / num_coins)
    remainder = (wallet_balance - fee) % final_coin_amt
    additions = []
    for i in range(num_coins):
        amt = final_coin_amt if i != 0 else final_coin_amt + remainder
        next_ph = await manager.wallet_client.get_next_address(manager.main_wallet_id, True)
        additions.append({"puzzle_hash": decode_puzzle_hash(next_ph), "amount": amt})

    tot = sum([x["amount"] for x in additions])
    assert tot == wallet_balance - fee
    tx = await manager.wallet_client.send_transaction_multi(
        manager.main_wallet_id,
        additions,
        xch_coins,
        fee,
    )

    await manager.wait_for_tx(tx.name)
    await manager.close()


# --------------------------------
# ENTRY AND RECOVERY FUNCTIONS
# --------------------------------


async def process_chunk():
    start_row = get_state()
    new_nft_ids = await mint(start_row, royalty_address, royalty_percentage)
    set_nfts_to_update(new_nft_ids)

    await asyncio.sleep(5)  # sleep for 5 seconds so wallet can catch up
    
    try:
        await update(new_nft_ids)
    except:
        raise ValueError("Set DID failed for chunk starting at row {}".format(start_row))

    await asyncio.sleep(5)  # sleep for 5 seconds so wallet can catch up
    
    try:
        await transfer(start_row)
    except:
        raise ValueError("Transfer Failed for chunk starting at row {}".format(start_row))

    set_state(start_row + chunk)
    print("\nMinted and transferred coins from csv rows: %s to %s" % (start_row, start_row + chunk))


async def retry_update():
    new_nft_ids = get_nfts_to_update()
    start_row = get_state()
    print("Retrying Set DID for rows: {} - {}".format(start_row, start_row + chunk))
    try:
        await update(new_nft_ids)
    except Exception as e:
        raise ValueError("Retry Set DID failed: \n{}".format(e))

    await retry_transfer()


async def retry_transfer():
    start_row = get_state()
    print("Retrying Transfer for rows: {} - {}".format(start_row, start_row + chunk))
    try:
        await transfer(start_row)
    except Exception as e:
        raise ValueError("Retry transfer failed: \n{}".format(e))
    set_state(start_row + chunk)


async def main():
    if not Path("mint_state.csv").is_file():
        set_state(0)

    max_rows = 10000

    while True:
        await process_chunk()
        next_row = get_state()
        if next_row + chunk >= max_rows:
            await process_chunk()
            break

    print("All NFTs have been minted and transferred")


# --------------------------------
# STATE MANAGEMENT FUNCTIONS
# --------------------------------


def get_state():
    with open("mint_state.csv", "r") as f:
        start_row = int(f.read())
    return start_row


def set_state(start_row):
    with open("mint_state.csv", "w") as f:
        f.write(str(start_row))


def set_minted(metadata_list):
    with open("completed_mints.csv", "a") as f:
        writer = csv.writer(f)
        writer.writerows(metadata_list)


def get_minted():
    with open("completed_mints.csv", "r") as f:
        metadata_list = f.read().splitlines()
    metadata_list = [r.split(",") for r in metadata_list]
    return metadata_list


def set_nfts_to_update(new_nft_ids):
    with open("nft_ids_to_update.csv", "w") as f:
        writer = csv.writer(f)
        for nft in new_nft_ids:
            writer.writerow([nft])

def get_nfts_to_update():
    with open("nft_ids_to_update.csv", "r") as f:
        new_nft_ids = f.read().splitlines()
    return new_nft_ids

def set_targets_sent(targets):
    with open("targets_sent.csv", "w") as f:
        writer = csv.writer(f)
        for target in targets:
            writer.writerow([target])

def get_targets_sent():
    with open("targets_sent.csv", "r") as f:
        targets_sent = f.read().splitlines()
    return targets_sent

async def test():
    manager = NFTManager()
    await manager.connect()
    await manager.check_wallet_sync()
    await manager.check_mempool()
    print("\nWallet Client and Node Client are connected :)\n")
    await manager.close()


if __name__ == "__main__":
    logging.basicConfig(
        filename="bulk_mint.log",
        filemode="a",
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    fee = 100
    if len(sys.argv) > 1:
        if sys.argv[1] == "view":
            asyncio.run(view())
        elif sys.argv[1] == "test":
            asyncio.run(test())
        elif sys.argv[1] == "split":
            num_coins = 30
            asyncio.run(split(num_coins, fee))
        elif sys.argv[1] == "clear":
            asyncio.run(clear_nfts())
        elif sys.argv[1] == "retry_update":
            asyncio.run(retry_update())
        elif sys.argv[1] == "retry_transfer":
            asyncio.run(retry_transfer())
    else:
        asyncio.run(main())
