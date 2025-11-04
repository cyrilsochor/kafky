# kafky

Kafky is a lightweight CLI tool for producing and consuming Apache Kafka messages using a YAML configuration and YAML input/ouput files. It aims to provide a simple way to send and read messages from Kafka topics with configuration stored in human-friendly YAML files.

## Features

- Consume messages from Kafka topics
- Produce messages to Kafka topics possibe using previos consumed messages as template
- Simple CLI usage for quick testing and automation
- Configuration-driven: change behavior by editing a YAML file

## Quick start

1. Clone the repository:
```bash
git clone https://github.com/cyrilsochor/kafky.git
cd kafky
```

2. Build using gradle wrapper. 
```bash
./gradlew assemble
```

3. Create a YAML configuration file (example below) and use the CLI to produce or consume messages.

## Configuration (example)

Create `config.yml` with Kafka connection and behavior options:

```yaml
kafka:
  bootstrap_servers:
    - "localhost:9092"
  security:
    # Example placeholders for TLS/SASL configuration
    # tls_enabled: false
    # sasl_mechanism: plain
    # sasl_username: user
    # sasl_password: pass

producer:
  topic: "example-topic"
  acks: "all"     # or 0, 1, all
  retries: 3

consumer:
  topic: "example-topic"
  group_id: "kafky-consumers"
  auto_offset_reset: "earliest"  # or latest
```

Adjust keys to match the fields implemented by this project.

## Usage

Examples assume a `kafky` executable in your PATH.

Produce a single message:
```bash
kafky produce -c config.yml -m "Hello, Kafka!"
```

Produce messages from a file (one message per line):
```bash
kafky produce -c config.yml -f messages.txt
```

Consume messages (prints to stdout):
```bash
kafky consume -c config.yml
```

Consume from beginning:
```bash
kafky consume -c config.yml --from-beginning
```

Options and flags may vary depending on the implementation in this repository; run `kafky --help` to see the exact CLI options.

## Docker

If a Dockerfile is included in the repo, build and run with:
```bash
docker build -t kafky:latest .
docker run --rm -v "$(pwd)/config.yml:/app/config.yml" kafky:latest produce -c /app/config.yml -m "Hello from Docker"
```

## Logging & Debugging

- Enable verbose or debug output via CLI flags (e.g., `--debug`) if the binary supports them.
- Check Kafka broker logs/metrics for network or authentication issues.

## Contributing

Contributions are welcome. Please open issues or pull requests describing bug fixes, features, or documentation improvements. If you plan larger changes, open an issue first to discuss the design.

When contributing:
- Follow the repository's coding conventions.
- Add tests where appropriate.
- Keep commits small and descriptive.

## License

See the LICENSE file in the repository for license details. If there is no LICENSE file yet, please add one or ask the repository owner which license to use.

## Contact

If you have questions or need help, open an issue in this repository or contact the repository owner.
