# Data Dictionary — HR

Source: `rcd_data/generators/hr.py` (`HRGenerator`). Recruitment status
comes from `utils/state_machines.py::RecruitmentFunnel`. PII/retention
detail: [[ropa]] §6; [[data-classification-retention]].

## `attendance`

Weekday-only (Mon–Fri), sampled for up to 500 employees to bound volume.

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `employee_id` | uuid FK → `employees.id` | |
| `date` | date | |
| `check_in`, `check_out` | string (HH:MM:SS), nullable | Only set for `present`/`remote` |
| `status` | enum | present, absent, late, remote, holiday, sick_leave |
| `hours_worked`, `overtime_h` | decimal | |

## `performance_reviews`

One row per employee per generated period, up to 1,000 employees.

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `employee_id` | uuid FK → `employees.id` | Reviewee — sampled without replacement, so at most one review per employee per run |
| `reviewer_id` | uuid FK → `employees.id` | Sampled independently — **not validated** against `employees.manager_id` |
| `period` | string | `"{year}-H{1|2}"` |
| `score` | decimal | Normal(3.5, 0.8), clipped [1, 5] |
| `rating` | enum | exceeds_expectations, meets_expectations, below_expectations, needs_improvement |
| `comments` | string | Placeholder text |
| `reviewed_at` | date | |

## `training_records`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `employee_id` | uuid FK → `employees.id` | Sampled with replacement — an employee can appear multiple times |
| `course_name` | string | `"Course {seq}: {topic}"` |
| `provider` | enum | Coursera, Udemy, LinkedIn Learning, Internal, AWS Training, Google Cloud, Databricks Academy |
| `started_at` | date | |
| `completed_at` | date, nullable | ~85% completion rate |
| `score` | decimal, nullable | Only if completed |
| `passed` | boolean | `score >= 70` |
| `credits` | integer | 1–9 |

## `recruitment_pipeline`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `position` | string | One of 11 fixed titles |
| `department` | enum | 11 departments (subset of `config.yaml`'s 13 — excludes Legal, Security) |
| `candidate_name` | string | **Placeholder, not a real name** — `"Candidate {seq}"` |
| `candidate_email` | string | **Placeholder** — `"candidate{i}@email.com"` |
| `status` | string | Terminal state from `RecruitmentFunnel` |
| `applied_at` | timestamp | |
| `hired_at` | timestamp, nullable | Only when `status = "hired"` |
| `recruiter_id` | uuid FK → `employees.id` | |
| `source` | enum | linkedin, referral, job_board, direct, agency |

Note: hired candidates are **not** linked forward into `employees` — there's
no `candidate_id`/`employee_id` bridge, so a "hired" row here doesn't
guarantee a matching `employees` row exists.

## `engagement_surveys`

| Column | Type | Description |
|---|---|---|
| `id` | uuid | Primary key |
| `employee_id` | uuid FK → `employees.id` | Sampled with replacement, up to 2,000 responses |
| `period` | string | `"{year}-Q{1..4}"` |
| `engagement_score`, `satisfaction_score` | decimal | Normal-clipped [1, 5] |
| `nps` | integer | -100 to 100 |
| `would_recommend` | boolean | ~72% true |
| `submitted_at` | date | |
