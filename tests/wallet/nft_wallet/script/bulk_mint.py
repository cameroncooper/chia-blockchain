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


config = load_config(Path(DEFAULT_ROOT_PATH), "config.yaml")
testnet_agg_sig_data = config["network_overrides"]["constants"]["testnet10"]["AGG_SIG_ME_ADDITIONAL_DATA"]
DEFAULT_CONSTANTS = DEFAULT_CONSTANTS.replace_str_to_bytes(**{"AGG_SIG_ME_ADDITIONAL_DATA": testnet_agg_sig_data})


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

        did_wallets = await self.wallet_client.get_wallets(wallet_type=WalletType.DECENTRALIZED_ID)
        nft_wallets = await self.wallet_client.get_wallets(wallet_type=WalletType.NFT)
        self.did_wallet_id = did_wallets[0]["id"]
        self.nft_wallet_id = nft_wallets[0]["id"]

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
        new_nfts = ["0x" + coin.name().hex() for coin in sb.additions() if coin.parent_coin_info != did_coin_id and coin.amount == 1]
        return  new_nfts

    async def mint_nfts(self, data, royalty_address, royalty_percentage, fee):
        did_id = await self.current_did_id()
        metadata_list = []
        for row in data:
            metadata = {
                "hash": row[0],
                "uris": [row[1]],
                "meta_hash": row[2],
                "meta_urls": [row[3]],
                "license_hash": row[4],
                "license_urls": [row[5]],
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
        resp = await self.wallet_client.bulk_set_nft_did(
            self.nft_wallet_id,
            did_id,
            nft_id_list,
            fee
        )
        if not resp["success"]:
            logging.error("Set DID failed for rows %s to %s: %s" % (start_row, end_row, resp))
            raise ValueError("Set DID failed for rows %s to %s" % (start_row, end_row))
        return resp

    async def transfer_nfts(self, nft_transfer_list, fee):
        resp = await self.wallet_client.bulk_nft_transfer(
            self.nft_wallet_id,
            nft_transfer_list,
            fee
        )
        if not resp["success"]:
            logging.error("Transfer failed for rows %s to %s: %s" % (start_row, end_row, resp))
            raise ValueError("Transfer failed for rows %s to %s" % (start_row, end_row))
        return resp


    async def wait_for_tx(self, tx_id):
        while True:
            tx = await self.wallet_client.get_transaction(1, tx_id)
            if tx.confirmed:
                break
            else:
                print("waiting for transaction")
                await asyncio.sleep(20)
                

async def mint(csv_filename, start_row, chunk, royalty_address, royalty_percentage, fee) -> None:
    with open(csv_filename, "r") as f:
        csv_reader = csv.reader(f)
        bulk_data = list(csv_reader)
    manager = NFTManager()
    await manager.connect()

    data = bulk_data[start_row:start_row+chunk]
    mint_resp = await manager.mint_nfts(data, royalty_address, royalty_percentage, fee)
    mint_sb = SpendBundle.from_json_dict(mint_resp["spend_bundle"])
    new_nft_ids = await manager.get_new_nfts(mint_sb)
    mint_tx = bytes32(hexstr_to_bytes(mint_resp["tx_id"][2:]))
    await manager.wait_for_tx(mint_tx)

    await manager.close()
    return new_nft_ids

async def update(new_nft_ids, fee):
    manager = NFTManager()
    await manager.connect()
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

async def transfer(target_filename, start_row, chunk, fee) -> None:
    with open(target_filename, "r") as f:
        target_data = f.read().splitlines()
    manager = NFTManager()
    await manager.connect()
    
    data = target_data[start_row:chunk]
    nfts = await manager.wallet_client.list_nfts(manager.nft_wallet_id)
    nft_ids = [info["nft_coin_id"] for info in nfts["nft_list"]]
    nft_transfer_list = list(zip(nft_ids, data))
    
    transfer_resp = await manager.transfer_nfts(nft_transfer_list, fee)
    transfer_sb = SpendBundle.from_json_dict(transfer_resp["spend_bundle"])
    transfer_tx = bytes32(hexstr_to_bytes(transfer_resp["tx_id"][2:]))
    await manager.wait_for_tx(transfer_tx)
    await manager.close()

    with open("completed_mints.csv", "a") as f:
        writer = csv.writer(f)
        writer.writerows(nft_transfer_list)

async def test():
    manager = NFTManager()
    await manager.connect()

    nft_data = await manager.wallet_client.list_nfts(3)
    nfts = nft_data["nft_list"]

    wallet_balance_res = await manager.wallet_client.get_wallet_balance(1)
    wallet_balance = wallet_balance_res["confirmed_wallet_balance"]
    
    xch_coins = await manager.wallet_client.select_coins(amount=wallet_balance, wallet_id=1)
    num_coins = 20
    fee = 100

    spendable_bal = sum([c.amount for c in xch_coins])
    dividable_bal = uint64(spendable_bal/num_coins)

    
    breakpoint()
    xch_coins = xch_coins_res["coins"]

    await manager.close()

async def main():
    csv_filename = "sample.csv"
    target_filename = "target_sample.csv"
    chunk = 10
    royalty_address = "did:chia:1t0nmt3cjeclutcyunmpwq9l4wak99g4wegs2f3kxu6vzwrhdeg7qkw9ne7"
    royalty_percentage = 300
    fee = 10

    with open(csv_filename, "r") as f:
        csv_reader = csv.reader(f)
        bulk_data = list(csv_reader)

    start_row = 0
    max_loops = 5
    for i in range(max_loops):
        print("Minting round: {}".format(i+1))
        new_nft_ids = await mint(
            csv_filename,
            start_row,
            chunk,
            royalty_address,
            royalty_percentage,
            fee
        )
        print("Setting DID round: {}".format(i+1))
        await update(new_nft_ids, fee)
        start_row += chunk

        completed_data = bulk_data[start_row:start_row+chunk]
        with open("completed_mints.csv", "a") as f:
            writer = csv.writer(f)
            writer.writerows(completed_data)

        

    
    start_row = 0
    for i in range(max_loops):
        print("Transfer round: {}".format(i+1))
        await transfer(target_filename, start_row, chunk, fee)
        

if __name__ == "__main__":
    logging.basicConfig(
        filename="bulk_mint.log",
        filemode='a',
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%H:%M:%S',
        level=logging.DEBUG
    )
    asyncio.run(main())
