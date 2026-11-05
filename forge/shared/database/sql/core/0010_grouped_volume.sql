-- Grouped quantities are not monetary turnover. Existing snapshots remain intact.
ALTER TABLE ranking.snapshots ALTER COLUMN comparison_currency DROP NOT NULL;
ALTER TABLE ranking.entries
    ADD COLUMN base_asset TEXT,
    ADD COLUMN group_rank INTEGER CHECK(group_rank > 0),
    ADD COLUMN volume_unit TEXT,
    ADD COLUMN volume_estimated BOOLEAN,
    ADD COLUMN volume_evidence JSONB CHECK(jsonb_typeof(volume_evidence)='object'),
    ADD CONSTRAINT ranking_volume_group_rank UNIQUE(snapshot_id,base_asset,group_rank),
    ADD CONSTRAINT ranking_volume_evidence CHECK(
        volume_evidence IS NULL OR
        (volume_estimated IS NOT NULL AND
         ((position IS NULL AND group_rank IS NULL) OR
          (position IS NOT NULL AND group_rank IS NOT NULL AND base_asset IS NOT NULL
           AND volume_unit=base_asset)))
    );
-- Existing ranking entry immutability triggers also protect these new fields.
