import asyncio
import csv
from secrets import token_bytes
from typing import Any, List

from faker import Faker

from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.bech32m import encode_puzzle_hash
from chia.wallet.did_wallet.did_info import DID_HRP

fake = Faker()


async def create_nft_sample() -> List[Any]:
    sample = [
        bytes32(token_bytes(32)).hex(),  # data_hash
        fake.image_url(),  # data_url
        bytes32(token_bytes(32)).hex(),  # metadata_hash
        fake.url(),  # metadata_url
        bytes32(token_bytes(32)).hex(),  # license_hash
        fake.url(),  # license_url
        1,  # edition_number
        1,  # edition_count
    ]
    return sample

async def create_target_sample() -> List[Any]:
    return [encode_puzzle_hash(bytes32(token_bytes(32)), "txch")]

async def main() -> None:
    count = 100
    coros = [create_nft_sample() for _ in range(count)]
    data = await asyncio.gather(*coros)
    with open("sample.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(data)
    royalty_did_hex = bytes32(token_bytes(32)).hex()
    royalty_did_address = encode_puzzle_hash(bytes32.from_hexstr(royalty_did_hex), DID_HRP)
    royalty_basis_pts = 300
    print("Royalty DID: %s" % royalty_did_address)
    print("Royalty Percent: %s" % royalty_basis_pts)

    target_coro = [create_target_sample() for _ in range(count)]
    target_data = await asyncio.gather(*target_coro)
    with open("target_sample.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerows(target_data)


if __name__ == "__main__":
    asyncio.run(main())
