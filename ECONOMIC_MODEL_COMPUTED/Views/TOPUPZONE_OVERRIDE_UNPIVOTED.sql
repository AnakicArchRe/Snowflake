create or replace view ACTUARIAL_ILS_POC.ECONOMIC_MODEL_COMPUTED.TOPUPZONE_OVERRIDE_UNPIVOTED(
	TOPUPZONEID,
	SCENARIOID,
	PRODUCTGROUP,
	SHAREFACTOR,
	PREMIUMFACTOR
) as
        with productGroup as (
            select 
                $1 ProductGroup
            from 
                values 
                    ('LINTEAU'),
                    ('MIDDLE'),
                    ('TOPUP')
        )
        , unpivoted as (
            SELECT 
                topupzoneid, 
                scenarioid, 
                pg.productgroup,
                case pg.productgroup
                    when 'LINTEAU' then linteau_sharefactor
                    when 'MIDDLE' then middle_sharefactor
                    when 'TOPUP' then topup_sharefactor
                    else null
                end as shareFactor,
                case pg.productgroup
                    when 'LINTEAU' then linteau_premiumfactor
                    when 'MIDDLE' then middle_premiumfactor
                    when 'TOPUP' then topup_premiumfactor
                    else null
                end as premiumFactor
            FROM 
                economic_model_scenario.topupzone_override
                cross join productgroup pg
        )
        select 
            * 
        from 
            unpivoted
        where
            shareFactor is not null 
            or premiumFactor is not null
          ;