import requests
import argparse
import time

def get_txn_cnt(rpc: str):
  data="{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"getTransactionCount\",\"params\":[{\"commitment\":\"processed\"}]}"
  resp = requests.post(rpc, data=data, headers={"Content-Type": "application/json"})
  txn_cnt = resp.json()["result"]

  data="{\"id\":1,\"jsonrpc\":\"2.0\",\"method\":\"getSlot\",\"params\":[{\"commitment\":\"processed\"}]}"
  resp = requests.post(rpc, data=data, headers={"Content-Type": "application/json"})
  slot = resp.json()["result"]
  return (txn_cnt, slot)

def parse_args() -> argparse.Namespace:
  parser = argparse.ArgumentParser()
  parser.add_argument(
    "-r",
    "--rpc",
    required=True,
    type=str
  )
  parser.add_argument(
    "-t",
    "--time",
    required=True,
    type=int
  )
  args = parser.parse_args()
  return args

def tps(rpc: str, poll: int):
  while True:
    before_txn_cnt, slot0 = get_txn_cnt(rpc)
    before_time = time.time()
    time.sleep(poll)
    after_time = time.time()
    after_txn_cnt, slot1 = get_txn_cnt(rpc)
    txn_cnt_diff = int(after_txn_cnt - before_txn_cnt)
    time_diff = int(after_time - before_time)
    tps = int(txn_cnt_diff / time_diff)
    print(f"elapsed: {time_diff}, txns: {txn_cnt_diff}, tps: {tps}, curr slot: {slot1}")


def main():
  args = parse_args()
  tps(args.rpc, args.time)

if __name__ == "__main__":
  main()

