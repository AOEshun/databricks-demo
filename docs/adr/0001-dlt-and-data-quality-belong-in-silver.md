# DLT en data-quality-checks horen in Silver, niet in Bronze

Bronze laadt data zo dicht mogelijk bij de bron — geen type-casts, geen Expectations, geen quarantine. Silver is de eerste laag met **gevalideerde** data: type-fixes, snake_case-conformance, DLT Expectations en gepaarde `_quarantine`-tabellen leven daar. Dit volgt de Databricks-guidance ("Bronze maintains source structure as-is, Silver provides the matched, merged, conformed and cleansed Enterprise view") en voorkomt dat een mislukte expectation een Bronze-rij verliest die we later voor herverwerking nodig kunnen hebben.
