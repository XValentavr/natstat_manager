## Prerequisites

List any prerequisites needed to run your project, such as:
- Docker
- Docker Compose
- Python (mention the required version)
- Any other software or tools

## Installation

1. Build the Docker images:
   ```bash
   docker-compose build 
   ```
    OR
    ```bash
    make build
    ```
2. 
## Usage

### Starting the Project

To start all services defined in the Docker Compose file, run:

```bash
make start
```

### Stopping the Project

To stop all running services, use:

```bash
make stop
```

### Restarting a Specific Service

To restart a specific service:

```bash
make restart name=service_name
```

Replace `service_name` with the name of the service you want to restart. For example `pinn_com_pinnacle_1`

### Additional Makefile Commands

List and explain other Makefile commands that are available, such as:
- `make remove-volumes`: remove docker volumes
- `make docker-prune`: to prune images, container and volumes

### Additional project commands

1. use `ruff check .` to see code problems
2. use `ruff --fix` to fix code problems

If you want to use local database in the docker, change `.env` file with your local creds