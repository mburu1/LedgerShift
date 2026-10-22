"""CLI for creating the local synthetic source snapshot."""
import argparse
import json

from ledgershift.generator.seed import generate


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["seed", "all"])
    parser.add_argument("--customers", type=int)
    parser.add_argument("--orders", type=int)
    parser.add_argument("--seed", type=int)
    args = parser.parse_args()
    manifest = generate(customers=args.customers, orders=args.orders, seed=args.seed)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
