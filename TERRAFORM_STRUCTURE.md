# Terraform Project Structure — Ride-Share Streaming

Infrastructure as code for the ride-share platform. Terraform provisions all AWS resources, state is
stored remotely in S3 with DynamoDB locking, and everything is driven by GitHub Actions with OIDC
authentication (no long-lived AWS keys). The whole stack can be created with `terraform apply` and
destroyed with a single manual GitHub Actions trigger.

---

## Directory layout

```
infrastructure/
├── main.tf                  # root module — wires modules together, provider config
├── variables.tf             # cluster_name, region, instance sizes, tags
├── outputs.tf               # EKS endpoint, ECR repo URLs, RDS host, app_url
├── terraform.tfvars         # actual values (GITIGNORED — never commit)
├── backend.tf               # S3 remote state + DynamoDB lock config
├── versions.tf              # required_providers + version pins
└── modules/
    ├── vpc/                 # VPC, public/private subnets, NAT, route tables
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    ├── ecr/                 # one ECR repo per service + lifecycle policy
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    ├── eks/                 # EKS cluster, managed node group, OIDC provider
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    ├── rds/                 # Postgres db.t3.micro in private subnet
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    ├── elasticache/         # Redis cache.t3.micro in private subnet
    │   ├── main.tf
    │   ├── variables.tf
    │   └── outputs.tf
    └── iam/                 # GitHub OIDC provider + CI/CD assume-role
        ├── main.tf
        ├── variables.tf
        └── outputs.tf
```

> Kafka is **not** a Terraform module. To save ~$150/month versus AWS MSK, Kafka runs inside EKS as
> the `bitnami/kafka` Helm chart, deployed by the CD pipeline after the cluster exists.

---

## Remote state (set up first, once)

Before writing any modules, create the S3 bucket and DynamoDB lock table manually (chicken-and-egg:
Terraform can't store its own bootstrap state remotely). Then point Terraform at them.

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket         = "rideshare-tf-state-yourname"
    key            = "prod/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "rideshare-tf-lock"
    encrypt        = true
  }
}
```

- **S3** holds the state file so it's never local — required for GitHub Actions to apply.
- **DynamoDB** provides a state lock so two pipeline runs can't apply simultaneously.

---

## Module responsibilities

| Module        | Creates                                                                 | Key cost-saving choice                    |
|---------------|-------------------------------------------------------------------------|-------------------------------------------|
| `vpc`         | VPC, 2 public + 2 private subnets, NAT gateway, route tables            | Single NAT gateway (not one per AZ)       |
| `ecr`         | One repo per service (7 repos), lifecycle policy keeping last 5 images  | Lifecycle policy prevents storage creep   |
| `eks`         | EKS cluster, managed node group (2× t3.medium), OIDC provider for IRSA  | Smallest viable node size, 2-node minimum |
| `rds`         | Postgres db.t3.micro, private subnet, SG allowing only EKS nodes        | t3.micro tier, no Multi-AZ in dev         |
| `elasticache` | Redis cache.t3.micro, private subnet                                    | t3.micro tier, single node               |
| `iam`         | GitHub OIDC identity provider + assume-role for CI/CD                   | OIDC = no static keys to rotate or leak   |

---

## Root module wiring

```hcl
# main.tf (simplified)
module "vpc" {
  source = "./modules/vpc"
  cidr   = var.vpc_cidr
  region = var.region
}

module "ecr" {
  source   = "./modules/ecr"
  services = ["gps-producer", "location-aggregator", "trip-event-service",
              "surge-pricing-engine", "notification-service", "dlq-processor", "api-gateway"]
}

module "eks" {
  source          = "./modules/eks"
  cluster_name    = var.cluster_name
  private_subnets = module.vpc.private_subnet_ids
  node_instance   = "t3.medium"
  min_nodes       = 2
  max_nodes       = 4
}

module "rds" {
  source          = "./modules/rds"
  private_subnets = module.vpc.private_subnet_ids
  eks_node_sg     = module.eks.node_security_group_id
  instance_class  = "db.t3.micro"
}

module "elasticache" {
  source          = "./modules/elasticache"
  private_subnets = module.vpc.private_subnet_ids
  node_type       = "cache.t3.micro"
}

module "iam" {
  source     = "./modules/iam"
  github_repo = "YOUR_GITHUB_USERNAME/rideshare-streaming"
}
```

---

## IAM / OIDC — no long-lived keys

GitHub Actions assumes a role via OIDC instead of storing AWS access keys.

```hcl
# modules/iam/main.tf
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

resource "aws_iam_role" "github_actions" {
  name = "rideshare-github-actions"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRoleWithWebIdentity"
      Effect    = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Condition = {
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:*"
        }
      }
    }]
  })
}
```

The only GitHub Secret needed for AWS is `AWS_ROLE_ARN` (the role ARN — not credentials).

---

## GitHub Actions workflows

```
.github/workflows/
├── ci.yml         # push + PR → mvn test, build/push images (main only), terraform plan (PR)
├── deploy.yml     # after CI passes on main → terraform apply + kubectl rollout
└── destroy.yml    # manual only → terraform destroy (requires typing DESTROY)
```

### deploy.yml flow
1. `terraform init` (pulls remote state from S3)
2. `terraform plan -out=tfplan`
3. `terraform apply tfplan`
4. `aws eks update-kubeconfig`
5. `kubectl set image` + `kubectl rollout status` per service

### destroy.yml — the cost kill-switch
```yaml
name: Destroy infrastructure
on:
  workflow_dispatch:
    inputs:
      confirm:
        description: 'Type DESTROY to confirm'
        required: true
jobs:
  destroy:
    if: github.event.inputs.confirm == 'DESTROY'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_ARN }}
          aws-region: us-east-1
      - uses: hashicorp/setup-terraform@v3
      - run: terraform init
        working-directory: infrastructure
      - run: terraform destroy -auto-approve
        working-directory: infrastructure
```

Trigger it from **GitHub → Actions → Destroy infrastructure → Run workflow → type DESTROY**.

---

## Required GitHub Secrets

| Secret           | Value                                     | Notes                          |
|------------------|-------------------------------------------|--------------------------------|
| `AWS_ROLE_ARN`   | ARN of the OIDC assume-role               | Not credentials — just the ARN |
| `TF_STATE_BUCKET`| S3 bucket name for remote state           | Created during bootstrap       |
| `ECR_REGISTRY`   | `<account>.dkr.ecr.<region>.amazonaws.com`| Image registry base URL        |

---

## Daily workflow

```bash
# Sit down to work
cd infrastructure && terraform apply        # ~12 min, cluster comes up

# ...develop, push commits, watch CI/CD deploy automatically...

# Done for the day → zero out cost
# GitHub → Actions → Destroy infrastructure → Run workflow → DESTROY
```

| State            | Cost/day      |
|------------------|---------------|
| Applied (active) | ~$5.35/day    |
| Destroyed        | ~$0.05/day    |

> Optional: add a scheduled `destroy.yml` trigger (cron at midnight) as a safety net in case you
> forget to tear down manually.

---

## Apply order (Terraform handles dependencies, but conceptually)

```
1. S3 + DynamoDB        (manual bootstrap, once)
2. iam (OIDC)           (so GitHub Actions can authenticate)
3. vpc                  (network foundation)
4. ecr                  (image registry — can run in parallel with vpc)
5. eks                  (needs vpc subnets)
6. rds + elasticache    (need vpc private subnets + eks node SG)
7. Kafka Helm chart     (deployed by CD pipeline onto EKS, not Terraform)
8. App rollout          (kubectl, by CD pipeline)
```
