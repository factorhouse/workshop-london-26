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

- **Docker and Docker Compose**: This is the only technical requirement. All workshop components—including Kafka brokers, Kpow, and the Python-based lab applications—will be deployed as Docker containers.
- **Hardware**: 8GB RAM minimum (16GB recommended).
- **Operating System**: macOS or Linux. (Windows users must use WSL2).
- **Internet Connection**: Required for the initial download of Docker images.

### Kpow Trial License

This workshop uses Kpow to manage and monitor your Kafka ecosystem. You will need a free trial license to activate the platform.

1.  **Generate Your License**: Visit the[Factor House Getting Started](https://account.factorhouse.io/auth/getting_started) page to generate your personal trial license.
2.  **Create `license.env` File**:
    - Create a new file named `license.env`.
    - Copy the license environment variables provided by Factor House and paste them into this file.
    - You can use `license.env.example` in that same directory as a reference for the correct format.

---

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

**Step 2: Update `setup.env`**
Open `setup.env`, comment out the default **Generic** provider, and uncomment the **Slack** configuration with your valid URL:

```bash
# WEBHOOK_PROVIDER=generic
# WEBHOOK_URL=http://webhook-server:9000

WEBHOOK_PROVIDER=slack
WEBHOOK_URL=https://hooks.slack.com/services/TXXX/BXXX/XXXX
```

Updating these variables in your `setup.env` file ensures that all administrative actions (such as topic creations, configuration edits, and ACL modifications) are routed directly to your Slack channel for real-time operational transparency.

---

## Lab 2: Rapid Kafka Diagnostics (20 mins)

In this lab, we will tackle the "Context Gap" using a live Kafka producer and consumer setup. We'll simulate a "Silent Stall" scenario where a poison pill message blocks a specific partition. You will learn how to use Kpow's unified interface to quickly trace the anomaly from high-level broker metrics down to the exact malformed message, and resolve the issue instantly by skipping the bad offset using Staged Mutations.

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

## Lab 5: Prometheus Integration (10 mins)

Standard Kafka monitoring often suffers from a "Quality Gap" due to noisy, raw JMX metrics. In this optional module, we will explore Kpow's built-in, high-fidelity telemetry engine. We will walk through pre-built dashboards in Grafana Cloud to demonstrate how Kpow bypasses raw JMX to automatically calculate actionable, business-level metrics for your Kafka environment, topics, consumer groups, and Connect clusters.
