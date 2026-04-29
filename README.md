# [WORKSHOP TITLE]

Welcome to the **[WORKSHOP TITLE]**. As Apache Kafka usage scales within an organization, platform and engineering teams often face mounting challenges around ecosystem visibility, infrastructure security, and data integration. This workshop is designed to equip you with the strategies and tools necessary to transform your Kafka infrastructure from a reactive data pipe into a secure, self-service, and fully transparent platform.

Rather than relying on fragmented CLI scripts and disconnected tools, you will explore a unified approach to managing your entire streaming ecosystem. We will cover how to streamline day-to-day platform operations, ranging from resolving complex data bottlenecks and enforcing strict multi-tenant access controls to seamlessly deploying integration pipelines and establishing real-time governance trails. By the end of this session, you will have the practical knowledge required to confidently operate, monitor, and secure mature Kafka environments while safely empowering developer productivity.

## 🎯 Key Learning Objectives

By the end of this workshop, you will have hands-on experience with:

- **Operational Governance:** Implement immutable audit trails via webhooks to capture and route every administrative change for real-time transparency.
- **Rapid Incident Response:** Trace consumer stalls from high-level lag metrics down to the specific malformed message causing the bottleneck.
- **Secure Self-Service:** Configure RBAC and multi-tenant isolation to safely delegate control to developers while protecting core infrastructure.
- **Controlled Change Management:** Master "Staging" workflows to enforce mandatory administrative reviews before high-impact infrastructure changes take effect.
- **Pipeline Lifecycle:** Streamline the deployment and monitoring of Kafka Connect pipelines using both a unified UI and Enterprise APIs.
- **Advanced Observability:** Move beyond raw JMX metrics to visualize actionable, business-level telemetry using Prometheus and Grafana.

## Preparation (20 mins)

### Prerequisites

Ensure you have the following installed and configured before starting the workshop:

- **Docker and Docker Compose**: This is the only technical requirement. All workshop components including Kafka brokers, Kpow, and the Python-based lab applications will be deployed as Docker containers.
- **Hardware**: 8GB RAM minimum (16GB recommended).
- **Operating System**: macOS or Linux. (Windows users are recommended to use WSL2).
- **Internet Connection**: Required for the initial download of Docker images.

### Kpow Trial License

This workshop uses Kpow to manage and monitor your Kafka ecosystem. You will need a free trial license to activate the platform.

1.  **Generate Your License**: Visit the[Factor House Getting Started](https://account.factorhouse.io/auth/getting_started) page to generate your personal trial license.
2.  **Create `license.env` File**:
    - Create a new file named `license.env`.
    - Copy the license environment variables provided by Factor House and paste them into this file.
    - You can use `license.env.example` in that same directory as a reference for the correct format.

---

## Introduction to Worshop Content

### Clone the Workshop repository

The workshop content is hosted on GitHub. You need to clone the repository.

```bash
git clone https://github.com/factorhouse/workshop-london-26.git
```

Describe docker compose services and kpow configuration.

```
resources/
├── kpow
│   ├── config
│   │   └── setup.env
│   ├── jaas
│   │   ├── hash-jaas.conf
│   │   └── hash-realm.properties
│   ├── rbac
│   │   └── hash-rbac.yml
│   └── schema
│       ├── schema_jaas.conf
│       └── schema_realm.properties
│── docker-compose.yml
```

## Lab 1: Real-Time Audit Trail via Webhooks (20 mins)

Operating critical data infrastructure requires an immutable record of administrative changes. Using Kpow on Docker Compose, this lab explores how to close the "Governance Gap." We will configure Kpow’s Webhook Integration to capture state-changing actions (like creating or deleting topics) and instantly route these audit events into communication channels like Slack for real-time operational transparency.

### Webhook Configuration

In this lab, Kpow is configured to route audit logs to a webhook. This configuration is managed within the [`setup.env`](./setup.env) file.

Choose one of the following two paths:

#### Option A: Generic (Default)

No action is required. By default, Kpow routes audit logs to a local diagnostic server included in your Docker Compose setup.

- **Logs visible at:** `docker compose logs -f webhook-server`

#### Option B: Slack (Optional)

If you wish to see live audit alerts in your own Slack workspace during this lab, follow these step-by-step instructions to create a Slack app and update your configuration.

**Step 1: Configure the Slack App and Webhook**

1. **Create a Slack app**: Navigate to the [Slack API website](https://api.slack.com/apps) and click on "Create New App". Choose to create it "From scratch".
2. **Name your app and choose a workspace**: Provide a name for your application and select the Slack workspace where you want to post messages.
3. **Enable incoming webhooks**: In your app settings page, go to "Incoming Webhooks" under the "Features" section. Toggle the feature on and then click "Add New Webhook to Workspace".
4. **Select a channel**: Choose the channel where you want the Kpow notifications to be posted and click "Allow".
5. **Copy the webhook URL**: After authorizing, you will be redirected back to the webhook configuration page. Copy the newly generated webhook URL. This URL is what you will use to configure Kpow.

**Step 2: Update Kpow Configuration**

Open [`setup.env`](./resources/kpow/config/setup.env), comment out the default **Generic** provider, and uncomment the **Slack** configuration with your valid URL:

```bash
# WEBHOOK_PROVIDER=generic
# WEBHOOK_URL=http://webhook-server:9000

WEBHOOK_PROVIDER=slack
WEBHOOK_URL=https://hooks.slack.com/services/TXXX/BXXX/XXXX
```

Updating these variables in your `setup.env` file ensures that all administrative actions (such as topic creations, configuration edits, and ACL modifications) are routed directly to your Slack channel for real-time operational transparency.

After updating the configuration, you can start the Kafka environment as follows.

```bash
docker compose up -d
```

Once started, you can create a topic and delete it. The corresponding audit logs will appeaer in the webshook server log (`generic`) or in your Slack channel (`slack`).

Additionally Kpow provides user/audit logs.

User can check their logs on Kpow.

![](./images/lab1-user-log.png)

Admins can check all audit logs on Kpow.

![](./images/lab1-audit-log.png)

Throughtout the lab, you'll be more audit logs will be created.

---

## Lab 2: Rapid Kafka Diagnostics (20 mins)

In this lab, we will tackle the "Context Gap" using a live Kafka producer and consumer setup. We'll simulate a "Silent Stall" scenario where a poison pill message blocks a specific partition. You will learn how to use Kpow's unified interface to quickly trace the anomaly from high-level broker metrics down to the exact malformed message, and resolve the issue instantly by skipping the bad offset using Staged Mutations.

Explains the Kafka app configurations

```
resources/diagnostics/
├── consumer.py
├── docker-compose.yml
└── producer.py
```

First, deploy a Kafka producer and consumer apps.

```bash
docker compose -f resources/diagnostics/docker-compose.yml --profile all up -d
```

After your skipped offset, close the consumer apps because the consumer group status should be _Empty_ for a staged mutation to take place.

```bash
docker compose -f resources/diagnostics/docker-compose.yml --profile consumer down
```

Once the staged mutation (skip offset) is succeeded, restart the consumer apps.

```bash
docker compose -f resources/diagnostics/docker-compose.yml --profile consumer up -d
```

You'll see the consumer that subscribes the partition is no longer blocked and continue comume messages.

If you want to stop the Kafka apps,

```bash
docker compose -f resources/diagnostics/docker-compose.yml --profile all down
```

## Break (10 mins)

## Lab 3: RBAC and Multi-Tenancy in Action (20 mins)

This lab demonstrates how to safely delegate self-service capabilities across different teams without compromising security. By logging in as different user personas (Admin, Owner, Editor, and Reader), you will experience how Kpow enforces Role-Based Access Control (RBAC) and tenant isolation. You'll see these roles in action, from read-only topic inspection to staging topic creations that require admin approval.

### Tenant Isolation & Resource Visibility

The configuration ensures developers only see business-relevant data.

- **Global Tenant (Platform Team):**
  - **Visibility:** Complete visibility (`["*"]`).
  - **Purpose:** Platform administrators use this to monitor the health of the entire ecosystem, including Kpow's own internal state.
- **Tenant 1 (Engineering/Dev Teams):**
  - **Visibility:** Limited to `cluster-1`, all Connectors, and all Schemas.
  - **Exclusions:** All internal Kpow topics and consumer groups (`oprtr*`, `__oprtr*`) are explicitly hidden.
  - **Purpose:** Provides a noise-free environment where developers cannot see or accidentally modify the platform's underlying infrastructure.

### Role Permissions Matrix

| Action             | kafka-admins | kafka-owners | kafka-editors   | kafka-readers   |
| :----------------- | :----------- | :----------- | :-------------- | :-------------- |
| **BROKER_EDIT**    | Allow        | **Deny**     | **Deny**        | (Implicit Deny) |
| **ACL_EDIT**       | Allow        | **Deny**     | **Deny**        | (Implicit Deny) |
| **TOPIC_CREATE**   | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **TOPIC_EDIT**     | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **TOPIC_DELETE**   | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **TOPIC_PRODUCE**  | Allow        | Allow        | Allow           | (Implicit Deny) |
| **TOPIC_INSPECT**  | Allow        | Allow        | Allow           | Allow           |
| **GROUP_EDIT**     | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **GROUP_DELETE**   | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **BULK_ACTION**    | Allow        | Allow        | (Implicit Deny) | (Implicit Deny) |
| **CONNECT_CREATE** | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **CONNECT_EDIT**   | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **SCHEMA_CREATE**  | Allow        | Allow        | **Stage**       | (Implicit Deny) |
| **SCHEMA_EDIT**    | Allow        | Allow        | **Stage**       | (Implicit Deny) |

**Security Design Principles**

- **Mandatory Approval Process (Staging):** The `kafka-editors` role is designed for engineering staff performing daily operational tasks. While they can produce data and inspect topics, any structural changes, such as creating topics, modifying connectors, or updating schemas, are not applied immediately. These actions are **Staged**, requiring review and approval by an Admin or Owner before taking effect.
- **Infrastructure Lockdown:** To ensure cluster stability, both `kafka-owners` and `kafka-editors` are explicitly **Denied** the ability to modify Broker configurations. Only the Platform Team (`kafka-admins`) can change underlying hardware and cluster-level settings.
- **Centralized Security Governance:** To maintain a strict security perimeter, the ability to manage ACLs is restricted to the Platform Team. Both `kafka-owners` and `kafka-editors` are explicitly **Denied** the ability to modify security permissions, ensuring that access control remains a centralized administrative function.
- **Deny by Default:** The configuration follows a strict security baseline where any action not explicitly granted to a role is automatically blocked. This **Implicit Deny** ensures that restricted roles, such as `kafka-readers`, cannot perform any state-changing actions like producing data or creating resources.

## Lab 4: Kafka Connect Management (20 mins)

Explore how to deploy and manage data pipelines using both the Kpow UI and its enterprise API. We will walk through configuring a source connector via the UI to generate mock data, and deploying a sink connector via the API to write that data to MinIO. You'll learn how to monitor running tasks, verify the data flow, and properly clean up the connectors.

Explain connector configuration - orders-ui.json

```
$ tree resources/connector/config/
resources/connector/config/
├── orders-api.json
└── orders-ui.json
```

A Kafka connector can be deployed on the UI and API.

### Deploy via UI

1. Navigate to the **Connect** section and click _Create connector_ to get started.

![](./images/lab5-create-connector-01.png)

2. Select the _GeneratorSourceConnector_ connector

![](./images/lab5-create-connector-02.png)

3. Import the source connector configuration file ([`./resources/connector/config/orders-ui.json`](./resources/connector/config/orders-ui.json)) and hit _Create_.

![](./images/lab5-create-connector-03.png)

5. Once deployed, you can check the source connector and its tasks in the Kpow UI.

![](./images/lab5-create-connector-04.png)

### Deploy via API

Now, you will create a new connector using the Kpow API. It is the same connector but sending message to a different topic.

1. Generate base64 encoded value of an API key and set tenant header.

The workshop environment pre-configures several users. For this demo, we'll use the `owner:password` credentials. Also, multi-tenancy is configured in Kpow, every HTTP reqeust should specify a tenant where the user belongs to.

```bash
AUTH_HEADER=$(echo "Authorization: Basic $(echo -n 'owner:password' | base64)")
TENANT_HEADER="x-tenant-id: AppTeam"
```

2. Get Kafka Connect cluster ID

To manage connectors via the API, we first need the Connect cluster ID. We'll store it in a separate variable.

```bash
curl -s -H "$AUTH_HEADER" -H "$TENANT_HEADER" \
  http://localhost:3001/connect/v1/clusters
```

Example response:

```json
{
  "clusters": [
    {
      "id": "connect-connect1-C8T4oQvDRm-yA8R-q_zJww",
      "label": "Local Connect Cluster",
      "type": "apache_connect"
    }
  ],
  "metadata": {
    "tenant_id": "AppTeam"
  }
}
```

This `CONNECT_ID` will be used in subsequent API calls to manage connectors.

3. Create the Connector

Now, make a POST request with _GeneratorSourceConnector_ connector configuration ([`./resources/connector/config/orders-api.json`](./resources/connector/config/orders-api.json)).

```bash
CONNECT_ID="connect-connect1-C8T4oQvDRm-yA8R-q_zJww"

curl -s -i -X POST -H "$AUTH_HEADER" -H "$TENANT_HEADER" \
  -H "Accept:application/json" -H  "Content-Type:application/json" \
  http://localhost:3001/connect/v1/apache/$CONNECT_ID/connectors \
  -d @resources/connector/config/orders-api.json
```

Example response:

```json
{
  "name": "orders-api",
  "metadata": {
    "response_id": "2ad68a8d-cdd0-40db-86a3-1880977230c7",
    "cluster_id": "cluster-1",
    "is_staged": false,
    "connect_id": "connect-connect1-C8T4oQvDRm-yA8R-q_zJww",
    "tenant_id": "AppTeam"
  }
}
```

We can check the status of the API as shown below.

```bash
CONNECTOR_NAME="orders-api"

curl -s -H "$AUTH_HEADER" -H "$TENANT_HEADER" \
  http://localhost:3001/connect/v1/apache/$CONNECT_ID/connectors/$CONNECTOR_NAME
```

Example response:

```json
{
  "name": "orders-api",
  "type": "source",
  "state": "RUNNING",
  "worker_id": "localhost:8083",
  "class": "com.amazonaws.mskdatagen.GeneratorSourceConnector",
  "topics": [],
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "localhost:8083"
    },
    {
      "id": 1,
      "state": "RUNNING",
      "worker_id": "localhost:8083"
    }
  ],
  "metadata": {
    "connect_id": "connect-connect1-C8T4oQvDRm-yA8R-q_zJww",
    "tenant_id": "AppTeam"
  }
}
```

4. Delete the Connector

You can delete the connector with the API as follows.

```bash
curl -X DELETE -H "$AUTH_HEADER" -H "$TENANT_HEADER" \
  http://localhost:3001/connect/v1/apache/$CONNECT_ID/connectors/$CONNECTOR_NAME
```

Example response:

```json
{
  "metadata": {
    "response_id": "b3535d85-c0c2-4490-acd3-e3f2a5c3cc03",
    "cluster_id": "cluster-1",
    "is_staged": false,
    "connect_id": "connect-connect1-C8T4oQvDRm-yA8R-q_zJww",
    "tenant_id": "AppTeam"
  }
}
```

## Lab 5: Prometheus Integration (10 mins)

Standard Kafka monitoring often suffers from a "Quality Gap" due to noisy, raw JMX metrics. In this optional module, we will explore Kpow's built-in, high-fidelity telemetry engine. We will walk through pre-built dashboards in Grafana Cloud to demonstrate how Kpow bypasses raw JMX to automatically calculate actionable, business-level metrics for your Kafka environment, topics, consumer groups, and Connect clusters.

## Clean-Up Environment

You can clean up the environment as follows:

```bash
# Delete the Kafka apps in Lab 2 if not done so
docker compose -f resources/diagnostics/docker-compose.yml --profile all down

# Delete the Kafka environment
docker compose down
```
