"""CLI for pycdc."""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pycdc",
        description="Please Your Customer Direct Connect — find CRM gift ideas",
    )
    sub = parser.add_subparsers(dest="command")

    # Search
    search_p = sub.add_parser("search", help="Search for gift ideas")
    search_p.add_argument("query", nargs="?", default="", help="Search keyword")
    search_p.add_argument("--occasion", choices=[o.value for o in _occasions()])
    search_p.add_argument("--budget", choices=[b.value for b in _budgets()])
    search_p.add_argument("--no-web", action="store_true", help="Skip web search")
    search_p.add_argument("--json", action="store_true")

    # Recommend
    rec_p = sub.add_parser("recommend", help="Get recommendations for a customer")
    rec_p.add_argument("--name", required=True)
    rec_p.add_argument("--company", default="")
    rec_p.add_argument("--deal-value", type=float, default=0)
    rec_p.add_argument("--interests", nargs="*", default=[])
    rec_p.add_argument("--occasion", default="thank_you")

    # List occasions
    sub.add_parser("occasions", help="List all occasions")
    sub.add_parser("budgets", help="List all budget tiers")

    args = parser.parse_args(argv)

    if args.command == "search":
        from .client import GiftFinder
        from .models import Occasion, Budget
        finder = GiftFinder(enable_web=not args.no_web)
        occasion = Occasion(args.occasion) if args.occasion else None
        budget = Budget(args.budget) if args.budget else None
        ideas = finder.find(occasion=occasion, budget=budget, query=args.query)
        if args.json:
            print(json.dumps([i.to_dict() for i in ideas], indent=2))
        else:
            for i in ideas:
                print(f"  [{i.budget.value:>8}] {i.name}")
                print(f"           {i.price_range} — {i.description[:60]}")
                if i.personalization_tip:
                    print(f"           Tip: {i.personalization_tip}")
                print()

    elif args.command == "recommend":
        from .client import GiftFinder
        from .models import CustomerProfile, Occasion
        customer = CustomerProfile(
            name=args.name, company=args.company,
            deal_value=args.deal_value, interests=args.interests)
        finder = GiftFinder()
        print(finder.summary_for(customer))

    elif args.command == "occasions":
        for o in _occasions():
            print(f"  {o.value}")

    elif args.command == "budgets":
        for b in _budgets():
            low, high = b.range
            print(f"  {b.value:>8}  ${low}-${high}")

    else:
        parser.print_help()


def _occasions():
    from .models import Occasion
    return list(Occasion)

def _budgets():
    from .models import Budget
    return list(Budget)


if __name__ == "__main__":
    main()
