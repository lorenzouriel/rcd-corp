# RCD Corp — HR Handbook

In-universe policy for the `hr` domain (`rcd_data/generators/hr.py`) and
the employee master data in `master_data.py`. Schema:
[[data-dictionary/hr]], [[data-dictionary/master-data]]. Privacy treatment:
[[ropa]] §6.

## Levels & compensation bands

Two tracks, each a ladder of 5–7 levels
(`generators/master_data.py::SALARY_BY_LEVEL`, figures in BRL/year):

| Track | Levels | Band (low–high) |
|---|---|---|
| Individual Contributor | IC1 → IC7 | R$30k → R$300k |
| Management | M1 → M5 | R$65k → R$400k |

Employment types: full_time (80%), part_time (10%), contractor (10%).

## Attendance

Tracked Monday–Friday only. Status mix: present (70%), remote (15%), late
(5%), holiday (4%), absent (3%), sick_leave (3%). Standard day is ~8 hours;
overtime is anything logged above that.

## Performance reviews

Twice yearly (`{year}-H1` / `{year}-H2`), 1–5 scale, mapped to four
ratings: exceeds_expectations (20%), meets_expectations (60%),
below_expectations (15%), needs_improvement (5%). **Note for anyone
building a real review workflow off this pattern**: the synthetic
`reviewer_id` is not validated against the reviewee's actual
`manager_id` — a real system should enforce that a review's reviewer is
the employee's manager (or a delegated peer/skip-level) rather than sampling
independently.

## Learning & development

Training providers: Coursera, Udemy, LinkedIn Learning, Internal, AWS
Training, Google Cloud, Databricks Academy. ~85% completion rate once
started; a course "passes" at a 70+ score.

## Recruitment funnel

`applied → screening → interview → technical → offer → hired`, with
`rejected` (at any stage) and `declined` (post-offer) as alternate
terminals (`utils/state_machines.py::RecruitmentFunnel`). Sourcing mix:
LinkedIn (35%), referral (25%), job_board (20%), direct (10%), agency
(10%). Per [[data-classification-retention]], rejected-candidate PII should
be anonymized after 1 year.

## Engagement

Quarterly pulse survey: engagement score and satisfaction score (1–5),
NPS (-100 to 100), and a would-recommend flag (baseline ~72% positive).
Report these in aggregate wherever possible — see
[[data-classification-retention]] on minimizing individual-level exposure.
