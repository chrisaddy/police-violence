import prefect
from prefect import task, Flow
from pandera import check_types
from pandera.typing import Series, DataFrame
from violence.types import RawInput, Age


@task
@check_types
def raw_data() -> DataFrame[RawInput]:
    import pandas as pd

    logger = prefect.context.get("logger")

    data = pd.read_csv("police_violence.csv")

    logger.info(f"{data.shape=}")

    return data


@task
@check_types(inplace=True)
def victim_name(raw_data: DataFrame[RawInput]) -> Series[Name]
    return raw_data["name"]


@task
@check_types(inplace=True)
def victim_age(raw_data) -> Series[Age]:
    return raw_data["age"]



# 'gender', 'race', 'victim_image', 'date',
#        'street_address', 'city', 'state', 'zip', 'county', 'cause_of_death',
#        'circumstances', 'disposition_official', 'officer_charged', 'news_urls',
#        'signs_of_mental_illness', 'allegedly_armed', 'wapo_threat_level',
#        'wapo_flee', 'wapo_body_camera', 'encounter_type', 'initial_reason',
#        'officer_names', 'officer_races', 'call_for_service', 'latitude',
#        'longitude', 'agency_responsible', 'ori', 'wapo_armed', 'wapo_id',
#        'geography', 'mpv_id', 'fe_id', 'tract', 'urban_rural_uspsai',
#        'urban_rural_nchs', 'hhincome_median_census_tract',
#        'pop_total_census_tract', 'pop_white_census_tract',
#        'pop_black_census_tract', 'pop_native_american_census_tract',
#        'pop_asian_census_tract', 'pop_pacific_islander_census_tract',
#        'pop_other_multiple_census_tract', 'pop_hispanic_census_tract',
#        'congressional_district_113', 'congressperson_lastname',
#        'congressperson_firstname', 'congressperson_party',
#        'officer_known_past_shootings', 'off_duty_killing', 'prosecutor_head',
#        'prosecutor_race', 'prosecutor_gender', 'prosecutor_special',
#        'prosecutor_party', 'prosecutor_url', 'prosecutor_term',
#        'prosecutor_in_court', 'independent_investigation'],



if __name__ == "__main__":
    with Flow("police-violence-flow") as flow:
        raw = raw_data()
        victim_age(raw)

    flow.run()
