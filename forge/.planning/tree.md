marketforge/
├── engine/
├── forge/
│   ├── pyproject.toml
│   ├── startup/
│   ├── shared/
│   ├── historical/
│   ├── datasets/
│   ├── orchestration/
│   ├── features/
│   ├── lab/
│   │   ├── inspection/
│   │   ├── optimization/
│   │   ├── training/
│   │   ├── evaluation/
│   │   └── tracking/
│   ├── cli/
│   ├── tui/
│   └── tests/
├── research/
├── data/
└── docs/

research/
├── README.md
├── studies/
│   ├── engine-output-validation/
│   │   ├── README.md
│   │   ├── notebooks/
│   │   ├── configs/
│   │   ├── run.py
│   │   ├── notes.md
│   │   └── report.md
│   ├── imbalance-bar-optimization/
│   └── first-ml-baseline/
├── journal/
└── references/


marketforge/
├── engine/
│   ├── scripts/
│   └── ... existing Rust structure
├── forge/
│   ├── pyproject.toml
│   ├── scripts/
│   ├── tests/
│   └── ... agreed Python structure
├── research/
├── docs/
├── data/
├── README.md
└── LICENSE

https://zensical.org/docs/get-started/?utm_source=chatgpt.com
docs/
├── zensical.toml
├── requirements.txt
└── content/
    ├── index.md
    ├── getting-started/
    ├── tutorials/
    ├── guides/
    ├── architecture/
    ├── reference/
    ├── development/
    └── assets/
        ├── branding/
        ├── screenshots/
        └── diagrams/


| Section | What belongs there |
|---|---|
| **Getting started** | What MarketForge does, installation, requirements and the first successful run. |
| **Tutorials** | Complete examples, such as acquiring archives and producing a canonical dataset. |
| **Guides** | Specific tasks: configuring recording, selecting datasets, managing streams or inspecting outputs. |
| **Architecture** | Engine/Forge responsibilities, data flow, ownership, integrity and recovery concepts. |
| **Reference** | Commands, configuration fields, APIs, dataset schemas and protocols. |
| **Development** | Building, testing, repository structure and contribution instructions. |

current images/ contents would move into docs/content/assets/.


Documentation · Quick start · Showcase · Architecture · Contributing · Community

README
Identity	MarketForge logo, one clear sentence explaining its purpose, and a few useful status badges.
Navigation	Direct links to documentation, examples and contributing.
Highlights	Four or five concrete capabilities that explain why someone would use it.
MarketForge in action	One compelling demonstration, with a brief explanation and a link to reproduce it.
Getting involved	Quick-start link, contribution invitation, community link and license.

docs/content/
├── showcase/
│   ├── index.md
│   ├── historical-to-parquet.md
│   └── live-market-data.md
└── assets/
    ├── branding/
    └── screenshots/
        └── showcase/
