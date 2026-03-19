# Home Assistant – ESB Energy Integration

Disclaimer: This project is completely vibecoded, and automatic fetching from ESB is currently completely broken.

This repository contains a **Home Assistant custom component** that reads energy data from ESB CSV exports and exposes import/export sensors plus historical statistics for Home Assistant’s **Energy** dashboard.

## Features

* Upload a CSV in the integration config; the file is merged into a single consolidated CSV.
* Supports both **import** and **export** readings (register and interval types).
* Exposes separate sensors: `sensor.esb_energy_import_<mprn>` and `sensor.esb_energy_export_<mprn>`.
* Imports historical usage into Home Assistant statistics for the Energy dashboard.
* Automatic ESB fetching is currently broken and disabled by default.
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

Open <http://localhost:8123> and finish the HA setup. The sensors will appear as `sensor.esb_energy_import_<mprn>` and `sensor.esb_energy_export_<mprn>`.

## Manual Installation

1. Copy the `custom_components/esb_energy/` folder into your Home Assistant config directory.
2. Add the following to `configuration.yaml` (optional; UI flow is preferred):
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
