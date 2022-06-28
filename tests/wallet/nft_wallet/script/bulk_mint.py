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

logging.basicConfig(format="%(message)s", level=logging.DEBUG, stream=sys.stdout)

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
            logging.error("Minting failed for rows %s to %s" % (start_row, end_row))
            raise ValueError("Minting failed for rows %s to %s" % (start_row, end_row))

        return resp["tx_id"]

    async def set_did():
        pass

    async def wait_for_confirmation(tx_id):
        pass

    async def split_coins(coin_count: int) -> None:
        pass


async def main(csv_filename, chunk, royalty_address, royalty_percentage, fee) -> None:
    with open(csv_filename, "r") as f:
        csv_reader = csv.reader(f)
        bulk_data = list(csv_reader)
    manager = NFTManager()
    await manager.connect()
    breakpoint()

    data = bulk_data[:chunk]
    mint_tx = await manager.mint_nfts(data, royalty_address, royalty_percentage, fee)
    breakpoint()
    # start_row = 0
    # for data in bulk_data[start_row:start_row+chunk]:
    #     mint_sb = await manager.mint_nfts(data, royalty_address, royalty_percentage)
    #     minted_nfts = await manager.wait_for_confirmation(mint_sb)
    #     logging.info("Minted %s NFTS" % len(minted_nfts))

    #     update_resp = await manager.set_did(minted_nfts)
    #     if not update_resp["success"]:
    #         logging.error("Failed to set did for rows %s to %s" % (start_row, end_row))
    #         raise ValueError("Failed to set did for rows %s to %s" % (start_row, end_row))
    #     update_sb = update_resp["spend_bundle"]
    #     updated_nfts = await manager.wait_for_confirmation(update_sb)

    #     start_row += chunk

    await manager.close()


if __name__ == "__main__":
    csv_filename = "sample.csv"
    chunk = 10
    royalty_address = "did:chia:1t0nmt3cjeclutcyunmpwq9l4wak99g4wegs2f3kxu6vzwrhdeg7qkw9ne7"
    royalty_percentage = uint16(300)
    wallet_fingerprint = 1455603606
    fee = 10

    asyncio.run(main(csv_filename, chunk, royalty_address, royalty_percentage, did_wallet_id, nft_wallet_id, fee))
