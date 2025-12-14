# Persistent Message Broker

Persistent Message Broker is a reliable message queue written in **C# 14** using **.NET 10**. The project is designed to provide **durable message delivery** and **crash-safe persistence** while supporting concurrent producers and consumers. The system relies on a **Write-Ahead Log (WAL)** mechanism for persistent data storage and state recovery after failures.

## Content Table
- [Features](#features)
- [Public HTTP API](#api)
- [Configuration](#configuration)
- [Running](#running)
- [Overriding Configuration](#overriding-configuration)
- [Running Tests](#tests)
- [Author](#author)

<span id="features"></span>
## Features

- **Reliable Persistence (WAL)**  
  All core operations (`Enqueue`, `Ack`, `Dead`) are written to disk before any in-memory state changes, ensuring durability and crash safety.

- **Atomic Writes**  
  Uses atomic file write operations to prevent WAL corruption and guarantee consistency at the log segment level.

- **Graceful Shutdown on Critical Failures**  
  The broker guarantees a controlled shutdown if a persistence operation cannot be written to disk, preventing inconsistent state.

- **Thread-Safe In-Memory Queue**  
  An in-memory queue optimized for high concurrent load, allowing multiple producers and consumers to operate safely in parallel.

- **Message Acknowledgement and Redelivery**  
  Messages must be acknowledged after consumption. Otherwise, they are redelivered until the maximum delivery attempt limit is reached.

<span id="api"></span>
## 1. Public HTTP API

The broker exposes a simple HTTP API for interacting with messages in the queue.

### Publish Message

Adds a new message to the queue.

| Property | Description                                                                                                                        |
|----------|------------------------------------------------------------------------------------------------------------------------------------|
| **Method** | `POST`                                                                                                                             |
| **URL** | `/api/broker/publish`                                                                                                              |
| **Request Body** | Raw message payload (`application/octet-stream`) |
| **Responses** | `201 Created` — message successfully published<br>`400 Bad Request` — message size exceeds the allowed limit                       |

### 2. Consume Message

Retrieves a message from the queue.

| Property | Description                                                                                                   |
|----------|---------------------------------------------------------------------------------------------------------------|
| **Method** | `GET`                                                                                                         |
| **URL** | `/api/broker/consume`                                                                                         |
| **Response** | `200 OK` — returns the raw message payload (`application/octet-stream`)<br>`204 No Content` — queue is empty  |
| **Response Headers** | `X-Message-Id` (GUID, ID of the consumed message)<br>`X-Delivery-Attempts` (int, current delivery attempts) |

### 3. Acknowledge Message

Confirms successful message processing.

| Property | Description                                                                                                        |
|----------|--------------------------------------------------------------------------------------------------------------------|
| **Method** | `POST`                                                                                                             |
| **URL** | `/api/broker/ack/{messageId}`                                                                                      |
| **Path Parameter** | `messageId` — ID of the consumed message                                                                           |
| **Responses** | `200 OK` — message successfully acknowledged<br>`404 Not Found` — sent message with the specified ID was not found |

<span id="configuration"></span>
## Configuration

The default configuration for the broker is located in the `appsettings.json` file.  
It contains settings for WAL persistence, message options, requeue behavior, and expiration policies.

### Default `appsettings.json`

```json
{
  "MessageBroker": {
    "Wal": {
      "Directory": "/var/lib/messagebroker/data",
      "ResetOnStartup": false,
      "MaxWriteCountPerFile": 100000,
      "FileNaming": {
        "Extension": "log",
        "EnqueuePrefix": "enqueue",
        "AckPrefix": "ack",
        "DeadPrefix": "dead",
        "EnqueueMergedPrefix": "enqueue-merged",
        "AckMergedPrefix": "ack-merged",
        "DeadMergedPrefix": "dead-merged"
      },
      "GarbageCollector": {
        "CollectInterval": "00:05:00"
      },
      "Manifest": {
        "FileName": "manifest.json"
      }
    },
    "Broker": {
      "Message": {
        "MaxPayloadSize": 1048576,
        "MaxDeliveryAttempts": 3
      },
      "Requeue": {
        "RequeueInterval": "00:00:30"
      },
      "ExpiredPolicy": {
        "ExpirationTime": "00:02:00"
      }
    }
  }
}
```

### WAL Options

| Property | Description                                                                    |
|----------|--------------------------------------------------------------------------------|
| `Directory` | Path to store WAL files                                                        |
| `ResetOnStartup` | If `true`, existing WAL files are cleared on broker startup                    |
| `MaxWriteCountPerFile` | Maximum number of write operations per WAL file segment before rotation        |
| `FileNaming.Extension` | File extension for WAL segments                                                |
| `FileNaming.EnqueuePrefix` | Prefix for enqueue log files                                                   |
| `FileNaming.AckPrefix` | Prefix for ack log files                                                       |
| `FileNaming.DeadPrefix` | Prefix for dead message log files                                              |
| `FileNaming.EnqueueMergedPrefix` | Prefix for merged enqueue files                                                |
| `FileNaming.AckMergedPrefix` | Prefix for merged ack files                                                    |
| `FileNaming.DeadMergedPrefix` | Prefix for merged dead files                                                   |
| `GarbageCollector.CollectInterval` | Interval for running WAL garbage collection                                    |
| `Manifest.FileName` | Name of the manifest file storing WAL metadata                                 |

### Broker Options

| Property | Description |
|----------|-------------|
| `Message.MaxPayloadSize` | Maximum allowed message size in bytes |
| `Message.MaxDeliveryAttempts` | Maximum number of delivery attempts before moving a message to Dead state |
| `Requeue.RequeueInterval` | Interval for checking and requeueing unacknowledged messages |
| `ExpiredPolicy.ExpirationTime` | Time after which a sent but unacknowledged message is considered expired |

<span id="running"></span>
## Running

This section explains how to run the Persistent Message Broker.

### Running the Broker Locally

To run the Persistent Message Broker locally, you need to have [.NET 10 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/10.0) installed on your machine.

#### Steps to run

1. **Clone the repository**
    ```bash
    git clone https://github.com/Andrii-Detix/Message-Broker.git
    ```
   
2. **Navigate to the project folder**
    ```bash
    cd Message-Broker
    ```

3. **Run the broker in Release mode on port 5122**
    ```bash
    dotnet run --project MessageBroker.Api --configuration Release --urls http://localhost:5122
    ```
   > By default, the broker uses the `appsettings.json` file located in the `MessageBroker.Api` folder for configuration.


### Running the Broker using Docker

You can run the Persistent Message Broker in a Docker container. Make sure you have [Docker](https://www.docker.com/get-started/) installed on your machine.

#### Steps to run

1. **Clone the repository**
    ```bash
    git clone https://github.com/Andrii-Detix/Message-Broker.git
    ```

2. **Navigate to the project folder**
    ```bash
    cd Message-Broker
    ```

3. **Build the Docker image**
    ```bash
    docker build -t message-broker:v1 -f MessageBroker.Api/Dockerfile .
    ```

4. **Run the container with a Data Volume**
    ```bash
   # Syntax: -v <HOST_PATH_FOR_DATA>:<CONTAINER_PATH_FOR_DATA>
    docker run -d --name message-broker -p 5122:8080 -v /path/to/host/wal:/var/lib/messagebroker/data message-broker:v1
    ```
   > To ensure data persistence and survivability of the WAL files, you must use a volume or bind mount.

<span id="overriding-configuration"></span>
## Overriding Configuration

You can override the default configuration of the broker differently depending on how you run it.

### Locally

#### 1. Pass parameters at startup

Override individual settings directly from the command line.

Format: `--<Section>:<Subsection>:<Property>=<Value>`

```bash
dotnet run --project MessageBroker.Api --configuration Release --urls http://localhost:5122 \
  --MessageBroker:Wal:Directory=<directory>/data \
  --MessageBroker:Broker:Message:MaxPayloadSize=2097152
```

#### 2. Use a custom configuration file

Provide a full custom appsettings.json.

Format: `--settings <directory>/<local-appsettings>.json`

```bash
dotnet run --project MessageBroker.Api --configuration Release --urls http://localhost:5122 \
  --settings <directory>/<local-appsettings>.json
```

### In Docker

#### 1. Pass parameters at startup

You can override configuration values when running the container with command-line parameters (similar to local).

Format: `--<Section>:<Subsection>:<Property>=<Value>`

```bash
docker run -d --name message-broker -p 5122:8080 -v /path/to/host/wal:/var/lib/messagebroker/data message-broker:v1 \
  --MessageBroker:Wal:Directory=/app/data/wal \
  --MessageBroker:Broker:Message:MaxPayloadSize=2097152
```

#### 2. Use environment variables

Set individual configuration values via environment variables.

Format: `-e <Section>__<Subsection>__<Property>=<Value>`

```bash
docker run -d --name message-broker -p 5122:8080 -v /path/to/host/wal:/var/lib/messagebroker/data \
  -e MessageBroker__Wal__Directory=/app/data/wal \
  -e MessageBroker__Broker__Message__MaxPayloadSize=2097152 \
  message-broker:v1
```

#### 3. Use a custom configuration file

Mount a custom configuration file inside the container.

Format: `-v <host-appsettings-path>:<docker-appsettings-path>`

```bash
docker run -d --name message-broker -p 5122:8080 -v /path/to/host/wal:/var/lib/messagebroker/data message-broker:v1 \
  -v /path/to/host/appsettings.json:/app/appsettings.json \
  message-broker:v1
```

<span id="tests"></span>
## Running Tests

This section explains how to run different types of tests for the Persistent Message Broker.

The project utilizes **xUnit** for testing. To ensure efficient development workflows, tests are categorized into standard Unit/Integration/E2E tests and Load tests (powered by NBomber).

All commands should be executed from the solution root.

### Prerequisites

Before running End-to-End tests you **must build the Docker image** used by Testcontainers.

```bash
docker build -t message-broker:test -f MessageBroker.Api/Dockerfile .
```

### Quick Run

Run all Unit, Integration, and End-to-End tests, excluding the heavy Load tests.

```bash
dotnet test --filter "Category!=Load"
```

### Load Tests

Run only the load tests.

**Note:** These tests are configured to run **sequentially (not in parallel)** to ensure system resources are isolated for accurate metrics.

```bash
dotnet test --filter "Category=Load"
```

### Run All Tests

Execute every test in the solution (including Load tests).

```bash
dotnet test
```

### Run a Specific Project

To run tests for a single project only (e.g., just the Unit tests).

```bash
dotnet test MessageBroker.UnitTests/MessageBroker.UnitTests.csproj
```

<span id="author"></span>
## Author

Developed by [Andrii Ivanchyshyn](https://github.com/Andrii-Detix).
