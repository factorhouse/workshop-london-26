# **[WORKSHOP TITLE]**

Welcome to the **[WORKSHOP TITLE]**. As Apache Kafka usage scales within an organization, platform and engineering teams often face mounting challenges around ecosystem visibility, infrastructure security, and data integration. This workshop is designed to equip you with the strategies and tools necessary to transform your Kafka infrastructure from a reactive data pipe into a secure, self-service, and fully transparent platform.

Rather than relying on fragmented CLI scripts and disconnected tools, you will explore a unified approach to managing your entire streaming ecosystem. We will cover how to streamline day-to-day platform operations, ranging from resolving complex data bottlenecks and enforcing strict multi-tenant access controls to seamlessly deploying integration pipelines and establishing real-time governance trails. By the end of this session, you will have the practical knowledge required to confidently operate, monitor, and secure mature Kafka environments while safely empowering developer productivity.

## Welcome & Preparation (20 mins)

## Lab 1: Rapid Kafka Diagnostics (20 mins)

In this lab, we will tackle the "Context Gap" using a live Kafka producer and consumer setup. We'll simulate a "Silent Stall" scenario where a poison pill message blocks a specific partition. You will learn how to use Kpow's unified interface to quickly trace the anomaly from high-level broker metrics down to the exact malformed message, and resolve the issue instantly by skipping the bad offset using Staged Mutations.

## Lab 2: Real-Time Audit Trail via Webhooks (20 mins)

Operating critical data infrastructure requires an immutable record of administrative changes. Using Kpow on Docker Compose, this lab explores how to close the "Governance Gap." We will configure Kpow’s Webhook Integration to capture state-changing actions (like creating or deleting topics) and instantly route these audit events into communication channels like Slack for real-time operational transparency.

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
