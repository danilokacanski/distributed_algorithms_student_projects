import argparse
import sys
import time
import json

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--experiment-id", required=True)
    parser.add_argument("--algorithm", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--max-steps", type=int, required=True)
    parser.add_argument("--simulate-failure", action="store_true")

    return parser.parse_args()


def main() -> int:
    args = parse_arguments()

    print(
        f"Starting {args.algorithm} on {args.environment} "
        f"with seed={args.seed} and max_steps={args.max_steps}.",
        flush=True,
    )

    progress_update_count = 4

    for update_index in range(1, progress_update_count + 1):
        time.sleep(0.5)

        current_step = (
            args.max_steps * update_index // progress_update_count
        )

        progress = {
            "currentStep": current_step,
            "metrics": {
                "mean_reward": 7.5 * update_index,
                "episodes": update_index,
            },
        }

        print(
            "PROGRESS_JSON:" + json.dumps(progress),
            flush=True,
        )

    if args.simulate_failure:
        print(
            "Python experiment failed intentionally.",
            file=sys.stderr,
            flush=True,
        )
        return 1
    
    metrics = {
        "total_reward": 150.0 + args.seed % 10,
        "mean_reward": 30.0 + args.seed % 5,
        "episodes": 5,
        "max_steps": args.max_steps,
    }

    print(
        "RESULT_JSON:" + json.dumps(metrics),
        flush=True,
    )
    print(
        f"Python experiment {args.experiment_id} completed successfully.",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())