# Home Assistant – ESB Energy Integration

This repository contains a **Home Assistant custom component** that reads energy consumption data from an ESB CSV export and exposes it as a sensor that can be used in Home Assistant’s **Energy** tab.

## Features

* Reads the latest reading from a CSV file you upload in the integration config.
* Exposes a single sensor: `sensor.esb_energy` with unit `kWh`.
* Works out‑of‑the‑box with Docker Compose for local testing.

## Quick Start (Docker)

```bash
# Clone the repo
git clone https://github.com/voytechnology/homeassistant-esb-energy.git
cd homeassistant-esb-energy

# Create a config directory and add your CSV file path
mkdir -p config
cat > config/configuration.yaml <<EOF
esb_energy:
  mprn: YOUR_MPRN_NUMBER
  csv_file: /config/esb_readings.csv
EOF

# Start Home Assistant
docker compose up -d
```

Open <http://localhost:8123> and finish the HA setup. The sensor will appear as `sensor.esb_energy`.

## Manual Installation

1. Copy the `custom_components/esb_energy/` folder into your Home Assistant config directory.
2. Add the following to `configuration.yaml`:
   ```yaml
   esb_energy:
     mprn: YOUR_MPRN_NUMBER
     csv_file: /config/esb_readings.csv
   ```
3. Restart Home Assistant.

## Development

```bash
# Install dependencies in a virtualenv
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests (if any) and linting:

```bash
pytest
flake8 custom_components/esb_energy/
```

## Contributing

Feel free to open issues or pull requests. Please follow the existing style.
