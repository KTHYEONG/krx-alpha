---
trigger:
  - on_label: ["quant"]
  - on_file_path_regex: "src/.*(core|strategy|execution|data|engine|features|validation).*"
  - on_file_path_glob: ["src/**/strategy/**/*.py", "src/**/execution/**/*.py", "src/**/data/**/*.py", "src/**/engine/**/*.py", "src/**/features/**/*.py", "src/**/validation/**/*.py"]
priority: 10
---

# Quant & Financial Engineering Principles

> **Never leak future information, preserve the reality of capital flows and execution viability, guard against validation leakage and overfitting, and prioritize economic correctness over specific implementation mechanics.**

## 1. Temporal Integrity & Information Availability (PIT & Leakage)
- **Information Availability:** Use strictly data that was realistically known and released at the decision timestamp. Never apply `.shift(1)` blindly without causal verification.
- **Point-in-Time (PIT) & Survivorship:** Ensure universes, historical constituents, and financial/filing disclosures contain no look-ahead restatements or survivorship bias (e.g., historical delistings must be preserved).
- **ML & Factor Validation Leakage:** Fit all learned preprocessing (scalers, encoders, PCA/factor orthogonalization) strictly on train folds. Apply purging/embargoing when target return horizons overlap across splits.

## 2. Execution Realism & Friction Accounting
- **Execution Viability & Friction:** Differentiate signal prices from realistically executable fill prices (spread, tick size constraints, auction dynamics, slippage). Account for transaction taxes (국내주식 거래세), brokerage commissions, and exchange fees.
- **Settlement & Cash Drag:** Account for T+2 settlement cycles and cash drag when evaluating long-only or long/short portfolio rebalancing and execution.
- **Portfolio Accounting Consistency:** Accurately reconcile cash, positions, fees, P&L, and external cash flows to avoid misrepresenting portfolio performance metrics (TWR, MWR, CAGR, Information Ratio).
- **Research-to-Production Parity:** Maintain consistent universe definitions, sizing logic, timing semantics, and feature engineering across research/backtesting and live execution.

## 3. Numerical Integrity & Economic Correctness
- **Numerical Edge Cases:** Handle division by zero, NaNs, and infinities according to their genuine market meaning (e.g., suspended trading, zero volume, unfillable orders) rather than silently coercing them into arbitrary normal values.
- **Metric Significance vs. Overfitting:** Avoid blindly tuning parameters against isolated metrics (IC, Sharpe, Rank IC); guard against selection bias and multi-testing p-hacking.
- **Principles Over Mechanics:** Prioritize sound financial and statistical meaning over rigid dogma around specific functions or recipes.
