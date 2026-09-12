# RCD Corp — Company Overview

In-universe reference for the fictional company `rcd_data` models. Grounded
in `config.yaml`'s `company` block and the product/org constants baked into
the generators — not invented beyond what those already imply.

## At a glance

| | |
|---|---|
| Name | RCD Corp |
| Ticker | RCDC |
| Founded | 2008 |
| Headquarters | São Paulo, Brazil |
| Markets | Brazil (BR), Mexico (MX), Portugal (PT), United States (US) |
| Locales | pt_BR, es_MX, pt_PT, en_US |
| Currencies | BRL, MXN, EUR, USD |

RCD Corp sells consumer electronics — smart-home, audio, and security
hardware under its own brands, plus resold third-party electronics — through
web, mobile app, physical retail, marketplaces, and call-center channels.

## Brand portfolio

RCD Corp's own product lines (`generators/master_data.py::RCD_PRODUCTS`):

| Brand | Category | Example products |
|---|---|---|
| NovaHome | Smart Home | SmartPlug Mini, Thermostat Pro, Hub X2 |
| PulseAudio | Audio | Earbuds Lite, Soundbar 5.1, Studio Headphones |
| GuardianIQ | Security | Cam 360, Doorbell Pro, Alarm Kit |

Alongside these, RCD Corp resells third-party electronics/appliances
(Samsung, LG, Sony, Philips, Electrolux, Dell) through the same channels.

## Footprint

- **Flagship retail**: RCD Paulista and RCD Ipanema (Brazil), RCD Reforma
  (Mexico City), RCD Chiado (Lisbon), RCD Brickell (Miami) — plus ~175
  additional stores of type flagship/standard/outlet/pop-up/online/warehouse.
- **Manufacturing**: three factories — São Paulo (45% of production
  volume), Manaus (35%), Monterrey (20%) — 20 machines each.
- **Distribution**: warehouses in São Paulo, Manaus, Monterrey, Lisbon,
  and Miami, typed as central/regional/dark-store.
- **Social presence**: 6 brand accounts (`@rcdcorp`, `@rcd_novahome`,
  `@pulseaudio_br`, `@guardian_iq`, `@rcd_online`, `@rcd_corp_pt`) active
  across Instagram, TikTok, YouTube, X, LinkedIn, and Facebook.

## Organization

13 departments (`config.yaml` → `company.departments`): Engineering,
Product, Data & Analytics, Marketing, Sales, Customer Success, Supply
Chain, Manufacturing, Finance, HR, Legal, IT, Security.

Two career tracks, each with its own compensation band (see
[[hr-handbook]]):

- **Individual contributor**: IC1 (entry) through IC7 (distinguished)
- **Management**: M1 (team lead) through M5 (executive)

## Where to go next

- [[data-privacy-lgpd]] / [[ropa]] / [[data-classification-retention]] —
  governance
- `foundation/data-dictionary/` — schema per domain
- [[finance-policies]], [[sales-playbook]], [[support-sla]],
  [[hr-handbook]], [[manufacturing-safety-qa]],
  [[marketing-brand-guidelines]] — how each function operates
