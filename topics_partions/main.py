import argparse
import runpy


def main():
    parser = argparse.ArgumentParser(description="Kafka topics and partitions demo")
    parser.add_argument("component", choices=["producer", "consumer"])
    args = parser.parse_args()

    if args.component == "producer":
        runpy.run_path("data_generator.py", run_name="__main__")
    else:
        runpy.run_path("consumer.py", run_name="__main__")


if __name__ == "__main__":
    main()