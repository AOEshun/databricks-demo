# DLT en data-quality-checks horen in Silver, niet in Bronze

> **Status update (2026-05-15):** Superseded by [ADR-0007](0007-quality-issues-are-routed-not-silently-dropped.md) and [ADR-0011](0011-quality-failed-rows-route-to-paired-dwq-table.md).
>
> The underlying principle — quality checks belong in the integration layer (formerly "Silver"), not pushed into datamart consumers — still holds. The mechanism is now more specific: quality-failed rows are *routed*, not silently dropped (ADR-0007), and they land in a paired `DWQ_<TABEL>` quarantine streaming table (ADR-0011). Read those ADRs for the binding decision.

Bronze laadt data zo dicht mogelijk bij de bron — geen type-casts, geen Expectations, geen quarantine. Silver is de eerste laag met **gevalideerde** data: type-fixes, snake_case-conformance, DLT Expectations en gepaarde `_quarantine`-tabellen leven daar. Dit volgt de Databricks-guidance ("Bronze maintains source structure as-is, Silver provides the matched, merged, conformed and cleansed Enterprise view") en voorkomt dat een mislukte expectation een Bronze-rij verliest die we later voor herverwerking nodig kunnen hebben.
