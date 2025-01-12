import os, os.path, subprocess
from constants import MEDI_FILE_TOKENS
import pandas as pd
import dask.dataframe as dd
from tqdm import tqdm
import ast
# E:/EHR-Work/yaoanlee/EHR_Linkage/LDS_ETL_CDM_v1/python_etl/Data/medi/linked_ehr/2018/


# ## location - care_site - person - death - provider - visit_occurrence - procedure_occurrence - condition_occurrence - device_exposure - measurement - drug_exposure - observation - observation_period - payer_plan_period - procedure_cost

# In[ ]:


#care_site
#condition_occurrence
#death
#device_exposure
#drug_exposure
#location
#measurement
#observation
#observation_period
#payer_plan_period
#person
#procedure_cost
#procedure_occurrence
#provider
#visit_occurrence


# # Pandas

# In[10]:
class OutcomeTransform(object):
    def __init__(self, BASE_OUTPUT_DIRECTORY, BASE_UPDATE_OUTPUT_DIRECTORY):
        self.BASE_OUTPUT_DIRECTORY = BASE_OUTPUT_DIRECTORY
        self.BASE_UPDATE_OUTPUT_DIRECTORY = BASE_UPDATE_OUTPUT_DIRECTORY


        print('Outcome Transforming Starts....')
        print('...BASE_OUTPUT_DIRECTORY            = ', BASE_OUTPUT_DIRECTORY)
        print('...BASE_UPDATE_OUTPUT_DIRECTORY     = ', BASE_UPDATE_OUTPUT_DIRECTORY)


        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'location_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'location_2018.csv')
        print('-------------------------LOCATION Transforming...---------------------')
        location = pd.read_csv(original_input_directory)
        location['location_id'] = location['location_id'].astype(str)
        if len(location)==len(location['county'].unique()):
            location_need_map = False
            location.to_csv(update_output_directory, index=False)
            del location
        else:
            location_need_map = True
            temp_location = location.groupby(['state','location_source_value']).agg({'location_id': ['max', list]}).reset_index()
            temp_location.columns = ['state','location_source_value', 'location_id_max', 'location_id_list']
            exploded = temp_location.explode('location_id_list')
            location_mapping = exploded.set_index('location_id_list')['location_id_max'].to_dict()
            updated_location = location.copy()
            updated_location['location_id'] = updated_location['location_id'].map(location_mapping)
            updated_location = updated_location.drop_duplicates()
            updated_location.to_csv(update_output_directory, index=False)
        
            del location, temp_location, updated_location, exploded
        
        
        # In[11]:
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'care_site_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'care_site_2018.csv')
        print('-------------------------CARE SITE Transforming...---------------------')
        care_site = pd.read_csv(original_input_directory)
        care_site['care_site_id'] = care_site['care_site_id'].astype(str)
        if len(care_site)==len(care_site['care_site_source_value'].unique()):
            care_site_need_map = False
            care_site.to_csv(update_output_directory, index=False)
            del care_site
        else:
            care_site_need_map = True
            temp_care_site = care_site.groupby(['place_of_service_concept_id','care_site_source_value']).agg({'care_site_id': ['max', list]}).reset_index()
            temp_care_site.columns = ['place_of_service_concept_id','care_site_source_value', 'care_site_id_max', 'care_site_id_list']
            exploded = temp_care_site.explode('care_site_id_list')
            care_site_mapping = exploded.set_index('care_site_id_list')['care_site_id_max'].to_dict()
            updated_care_site = care_site.copy()
            updated_care_site['care_site_id'] = updated_care_site['care_site_id'].map(care_site_mapping)
            updated_care_site = updated_care_site.drop_duplicates()
            updated_care_site.to_csv(update_output_directory, index=False)
        
            del care_site, temp_care_site, updated_care_site, exploded
        
        
        # In[12]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'person_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'person_2018.csv')
        print('-------------------------PERSON Transforming...---------------------')
        person = pd.read_csv(original_input_directory)
        person['person_id'] = person['person_id'].astype(str)
        if location_need_map == True:
            person[' location_id'] = person[' location_id'].astype(str)
            person[' location_id'] = person[' location_id'].map(location_mapping)
        
        if len(person)==len(person[' person_source_value'].unique()):
            person_need_map = False
            person.to_csv(update_output_directory, index=False)
            del person
        else:
            person_need_map = True
            temp_person = person.groupby([' person_source_value',' year_of_birth']).agg({'person_id': ['max', list]}).reset_index()
            temp_person.columns = [' person_source_value',' year_of_birth', 'person_id_max', 'person_id_list']
            exploded = temp_person.explode('person_id_list')
            person_mapping = exploded.set_index('person_id_list')['person_id_max'].to_dict()
            updated_person = person.copy()
            updated_person['person_id'] = updated_person['person_id'].map(person_mapping)
            updated_person = updated_person.drop_duplicates()
            updated_person.to_csv(update_output_directory, index=False)
        
            del person, temp_person, updated_person, exploded
        
        
        # In[13]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'death_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'death_2018.csv')
        print('-------------------------DEATH Transforming...---------------------')
        death = pd.read_csv(original_input_directory)
        if person_need_map == True:
            death['person_id'] = death['person_id'].astype(str)
            death['person_id'] = death['person_id'].map(person_mapping)
            death = death.drop_duplicates()
            death.to_csv(update_output_directory, index=False)
        else:
            death.to_csv(update_output_directory, index=False)
        
        del death
        
        
        # In[14]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'provider_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'provider_2018.csv')
        print('-------------------------PROVIDER Transforming...---------------------')
        provider = pd.read_csv(original_input_directory)
        provider['provider_id'] = provider['provider_id'].astype(str)
        if care_site_need_map == True:
            provider['care_site_id'] = provider['care_site_id'].astype(str)
            provider['care_site_id'] = provider['care_site_id'].map(care_site_mapping)
        
        if len(provider)==len(provider['provider_source_value'].unique()):
            provider_need_map = False
            provider.to_csv(update_output_directory, index=False)
            del provider
        else:
            provider_need_map = True
            temp_provider = provider.groupby(['provider_source_value']).agg({'provider_id': ['max', list]}).reset_index()
            temp_provider.columns = ['provider_source_value', 'provider_id_max', 'provider_id_list']
            exploded = temp_provider.explode('provider_id_list')
            provider_mapping = exploded.set_index('provider_id_list')['provider_id_max'].to_dict()
            updated_provider = provider.copy()
            updated_provider['provider_id'] = updated_provider['provider_id'].map(provider_mapping)
            updated_provider = updated_provider.drop_duplicates()
            updated_provider.to_csv(update_output_directory, index=False)
        
            del provider, temp_provider, updated_provider, exploded
        
        
        # In[20]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'visit_occurrence_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'visit_occurrence_2018.csv')
        print('-------------------------VISIT OCCURRENCE Transforming...---------------------')
        visit_occurrence = pd.read_csv(original_input_directory)
        visit_occurrence['person_id'] = visit_occurrence['person_id'].astype(str)
        visit_occurrence['provider_id'] = visit_occurrence['provider_id'].astype('Int64').astype('object')
        visit_occurrence['care_site_id'] = visit_occurrence['care_site_id'].astype(str)
        visit_occurrence['visit_occurrence_id'] = visit_occurrence['visit_occurrence_id'].astype(str)
        if person_need_map == True:
            visit_occurrence['person_id'] = visit_occurrence['person_id'].map(person_mapping)
        if provider_need_map == True:
            visit_occurrence['provider_id'] = visit_occurrence['provider_id'].map(provider_mapping)
        if care_site_need_map == True:
            visit_occurrence['care_site_id'] = visit_occurrence['care_site_id'].map(care_site_mapping)
        
        if len(visit_occurrence)==len(visit_occurrence['visit_source_value'].unique()):
            visit_occurrence_need_map = False
            visit_occurrence.to_csv(update_output_directory, index=False)
            del visit_occurrence
        else:
            visit_occurrence_need_map = True
            temp_visit_occurrence = visit_occurrence.groupby(['person_id', 'visit_source_value']).agg({'visit_occurrence_id': ['max', list]}).reset_index()
            temp_visit_occurrence.columns = ['person_id', 'visit_source_value', 'visit_occurrence_id_max', 'visit_occurrence_id_list']
            exploded = temp_visit_occurrence.explode('visit_occurrence_id_list')
            visit_occurrence_mapping = exploded.set_index('visit_occurrence_id_list')['visit_occurrence_id_max'].to_dict()
            updated_visit_occurrence = visit_occurrence.copy()
            updated_visit_occurrence['visit_occurrence_id'] = updated_visit_occurrence['visit_occurrence_id'].map(visit_occurrence_mapping)
            updated_visit_occurrence = updated_visit_occurrence.drop_duplicates()
            updated_visit_occurrence.to_csv(update_output_directory, index=False)
        
            del visit_occurrence, temp_visit_occurrence, updated_visit_occurrence, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_occurrence_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'procedure_occurrence_2018.csv')
        print('-------------------------PROCEDURE OCCURRENCE Transforming...---------------------')
        procedure_occurrence = pd.read_csv(original_input_directory)
        procedure_occurrence['person_id'] = procedure_occurrence['person_id'].astype(str)
        procedure_occurrence['provider_id'] = procedure_occurrence['provider_id'].astype(str)
        procedure_occurrence['visit_occurrence_id'] = procedure_occurrence['visit_occurrence_id'].astype(str)
        procedure_occurrence['procedure_occurrence_id'] = procedure_occurrence['procedure_occurrence_id'].astype(str)
        if person_need_map == True:
            procedure_occurrence['person_id'] = procedure_occurrence['person_id'].map(person_mapping)
        if provider_need_map == True:
            procedure_occurrence['provider_id'] = procedure_occurrence['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            procedure_occurrence['visit_occurrence_id'] = procedure_occurrence['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        if len(procedure_occurrence)==len(procedure_occurrence.drop_duplicates(subset=['person_id','visit_occurrence_id','procedure_source_value'])):
            procedure_occurrence_need_map = False
            procedure_occurrence.to_csv(update_output_directory, index=False)
            del procedure_occurrence
        else:
            procedure_occurrence_need_map = True
            temp_procedure_occurrence = procedure_occurrence.groupby(['person_id', 'visit_occurrence_id', 'procedure_source_value']).agg({'procedure_occurrence_id': ['max', list]}).reset_index()
            temp_procedure_occurrence.columns = ['person_id', 'visit_occurrence_id', 'procedure_source_value', 'procedure_occurrence_id_max', 'procedure_occurrence_id_list']
            exploded = temp_procedure_occurrence.explode('procedure_occurrence_id_list')
            procedure_occurrence_mapping = exploded.set_index('procedure_occurrence_id_list')['procedure_occurrence_id_max'].to_dict()
            updated_procedure_occurrence = procedure_occurrence.copy()
            updated_procedure_occurrence['procedure_occurrence_id'] = updated_procedure_occurrence['procedure_occurrence_id'].map(procedure_occurrence_mapping)
            updated_procedure_occurrence = updated_procedure_occurrence.drop_duplicates()
            updated_procedure_occurrence.to_csv(update_output_directory, index=False)
        
            del procedure_occurrence, temp_procedure_occurrence, updated_procedure_occurrence, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'condition_occurrence_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'condition_occurrence_2018.csv')
        print('-------------------------CONDITION OCCURRENCE Transforming...---------------------')
        condition_occurrence = pd.read_csv(original_input_directory)
        condition_occurrence['person_id'] = condition_occurrence['person_id'].astype(str)
        condition_occurrence['provider_id'] = condition_occurrence['provider_id'].astype(str)
        condition_occurrence['visit_occurrence_id'] = condition_occurrence['visit_occurrence_id'].astype(str)
        condition_occurrence['condition_occurrence_id'] = condition_occurrence['condition_occurrence_id'].astype(str)
        if person_need_map == True:
            condition_occurrence['person_id'] = condition_occurrence['person_id'].map(person_mapping)
        if provider_need_map == True:
            condition_occurrence['provider_id'] = condition_occurrence['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            condition_occurrence['visit_occurrence_id'] = condition_occurrence['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        subsets = list(condition_occurrence.columns)
        subsets.remove('condition_occurrence_id')
        if len(condition_occurrence)==len(condition_occurrence.drop_duplicates(subset=subsets)):
            condition_occurrence.to_csv(update_output_directory, index=False)
            del condition_occurrence
        else:
            temp_condition_occurrence = condition_occurrence.groupby(['person_id', 'visit_occurrence_id', 'condition_source_value']).agg({'condition_occurrence_id': ['max', list]}).reset_index()
            temp_condition_occurrence.columns = ['person_id', 'visit_occurrence_id', 'condition_source_value', 'condition_occurrence_id_max', 'condition_occurrence_id_list']
            exploded = temp_condition_occurrence.explode('condition_occurrence_id_list')
            condition_occurrence_mapping = exploded.set_index('condition_occurrence_id_list')['condition_occurrence_id_max'].to_dict()
            del exploded, temp_condition_occurrence
            condition_occurrence['condition_occurrence_id'] = condition_occurrence['condition_occurrence_id'].map(condition_occurrence_mapping)
            #condition_occurrence = condition_occurrence.drop_duplicates()
            condition_occurrence.to_csv(update_output_directory, index=False)
        
            del condition_occurrence, condition_occurrence_mapping
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'device_exposure_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'device_exposure_2018.csv')
        print('-------------------------DEVICE EXPOSURE Transforming...---------------------')
        device_exposure = pd.read_csv(original_input_directory)
        device_exposure['person_id'] = device_exposure['person_id'].astype(str)
        device_exposure['provider_id'] = device_exposure['provider_id'].astype(str)
        device_exposure['visit_occurrence_id'] = device_exposure['visit_occurrence_id'].astype(str)
        device_exposure['device_exposure_id'] = device_exposure['device_exposure_id'].astype(str)
        if person_need_map == True:
            device_exposure['person_id'] = device_exposure['person_id'].map(person_mapping)
        if provider_need_map == True:
            device_exposure['provider_id'] = device_exposure['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            device_exposure['visit_occurrence_id'] = device_exposure['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        subsets = list(device_exposure.columns)
        subsets.remove('device_exposure_id')
        if len(device_exposure)==len(device_exposure.drop_duplicates(subset=subsets)):
            device_exposure.to_csv(update_output_directory, index=False)
            del device_exposure
        else:
            temp_device_exposure = device_exposure.groupby(['person_id', 'visit_occurrence_id', 'device_source_value']).agg({'device_exposure_id': ['max', list]}).reset_index()
            temp_device_exposure.columns = ['person_id', 'visit_occurrence_id', 'device_source_value', 'device_exposure_id_max', 'device_exposure_id_list']
            exploded = temp_device_exposure.explode('device_exposure_id_list')
            device_exposure_mapping = exploded.set_index('device_exposure_id_list')['device_exposure_id_max'].to_dict()
            device_exposure['device_exposure_id'] = device_exposure['device_exposure_id'].map(device_exposure_mapping)
            device_exposure = device_exposure.drop_duplicates()
            device_exposure.to_csv(update_output_directory, index=False)
        
            del device_exposure, temp_device_exposure, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'measurement_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'measurement_2018.csv')
        print('-------------------------MEASUREMENT Transforming...---------------------')
        measurement = pd.read_csv(original_input_directory)
        measurement['person_id'] = measurement['person_id'].astype(str)
        measurement['provider_id'] = measurement['provider_id'].astype(str)
        measurement['visit_occurrence_id'] = measurement['visit_occurrence_id'].astype(str)
        measurement['measurement_id'] = measurement['measurement_id'].astype(str)
        if person_need_map == True:
            measurement['person_id'] = measurement['person_id'].map(person_mapping)
        if provider_need_map == True:
            measurement['provider_id'] = measurement['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            measurement['visit_occurrence_id'] = measurement['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        subsets = list(measurement.columns)
        subsets.remove('measurement_id')
        if len(measurement)==len(measurement.drop_duplicates(subset=subsets)):
            measurement.to_csv(update_output_directory, index=False)
            del measurement
        else:
            temp_measurement = measurement.groupby(['person_id', 'visit_occurrence_id', 'measurement_source_value']).agg({'measurement_id': ['max', list]}).reset_index()
            temp_measurement.columns = ['person_id', 'visit_occurrence_id', 'measurement_source_value', 'measurement_id_max', 'measurement_id_list']
            exploded = temp_measurement.explode('measurement_id_list')
            measurement_mapping = exploded.set_index('measurement_id_list')['measurement_id_max'].to_dict()
            measurement['measurement_id'] = measurement['measurement_id'].map(measurement_mapping)
            measurement = measurement.drop_duplicates()
            measurement.to_csv(update_output_directory, index=False)
        
            del measurement, temp_measurement, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'drug_exposure_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'drug_exposure_2018.csv')
        print('-------------------------DRUG EXPOSURE Transforming...---------------------')
        drug_exposure = pd.read_csv(original_input_directory)
        drug_exposure['person_id'] = drug_exposure['person_id'].astype(str)
        drug_exposure['provider_id'] = drug_exposure['provider_id'].astype(str)
        drug_exposure['visit_occurrence_id'] = drug_exposure['visit_occurrence_id'].astype(str)
        drug_exposure['drug_exposure_id'] = drug_exposure['drug_exposure_id'].astype(str)
        if person_need_map == True:
            drug_exposure['person_id'] = drug_exposure['person_id'].map(person_mapping)
        if provider_need_map == True:
            drug_exposure['provider_id'] = drug_exposure['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            drug_exposure['visit_occurrence_id'] = drug_exposure['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        subsets = list(drug_exposure.columns)
        subsets.remove('drug_exposure_id')
        if len(drug_exposure)==len(drug_exposure.drop_duplicates(subset=subsets)):
            drug_exposure.to_csv(update_output_directory, index=False)
            del drug_exposure
        else:
            temp_drug_exposure = drug_exposure.groupby(['person_id', 'visit_occurrence_id', 'drug_source_value']).agg({'drug_exposure_id': ['max', list]}).reset_index()
            temp_drug_exposure.columns = ['person_id', 'visit_occurrence_id', 'drug_source_value', 'drug_exposure_id_max', 'drug_exposure_id_list']
            exploded = temp_drug_exposure.explode('drug_exposure_id_list')
            drug_exposure_mapping = exploded.set_index('drug_exposure_id_list')['drug_exposure_id_max'].to_dict()
            drug_exposure['drug_exposure_id'] = drug_exposure['drug_exposure_id'].map(drug_exposure_mapping)
            drug_exposure = drug_exposure.drop_duplicates()
            drug_exposure.to_csv(update_output_directory, index=False)
        
            del drug_exposure, temp_drug_exposure, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'observation_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'observation_2018.csv')
        print('-------------------------OBSERVATION Transforming...---------------------')
        observation = pd.read_csv(original_input_directory)
        observation['person_id'] = observation['person_id'].astype(str)
        observation['provider_id'] = observation['provider_id'].astype('Int64').astype('object')
        observation['visit_occurrence_id'] = observation['visit_occurrence_id'].astype(str)
        observation['observation_id'] = observation['observation_id'].astype(str)
        if person_need_map == True:
            observation['person_id'] = observation['person_id'].map(person_mapping)
        if provider_need_map == True:
            observation['provider_id'] = observation['provider_id'].map(provider_mapping)
        if visit_occurrence_need_map == True:
            observation['visit_occurrence_id'] = observation['visit_occurrence_id'].map(visit_occurrence_mapping)
        
        subsets = list(observation.columns)
        subsets.remove('observation_id')
        if len(observation)==len(observation.drop_duplicates(subset=subsets)):
            observation.to_csv(update_output_directory, index=False)
            del observation
        else:
            temp_observation = observation.groupby(['person_id', 'visit_occurrence_id', 'observation_source_value']).agg({'observation_id': ['max', list]}).reset_index()
            temp_observation.columns = ['person_id', 'visit_occurrence_id', 'observation_source_value', 'observation_id_max', 'observation_id_list']
            exploded = temp_observation.explode('observation_id_list')
            observation_mapping = exploded.set_index('observation_id_list')['observation_id_max'].to_dict()
            observation['observation_id'] = observation['observation_id'].map(observation_mapping)
            observation = observation.drop_duplicates()
            observation.to_csv(update_output_directory, index=False)
        
            del observation, temp_observation, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'observation_period_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'observation_period_2018.csv')
        print('-------------------------OBSERVATION PERIOD Transforming...---------------------')
        observation_period = pd.read_csv(original_input_directory)
        observation_period['person_id'] = observation_period['person_id'].astype(str)
        observation_period['observation_period_id'] = observation_period['observation_period_id'].astype(str)
        if person_need_map == True:
            observation_period['person_id'] = observation_period['person_id'].map(person_mapping)
        
        subsets = list(observation_period.columns)
        subsets.remove('observation_period_id')
        if len(observation_period)==len(observation_period.drop_duplicates(subset=subsets)):
            observation_period.to_csv(update_output_directory, index=False)
            del observation_period
        else:
            temp_observation_period = observation_period.groupby(['person_id', 'observation_period_start_date', 'observation_period_end_date']).agg({'observation_period_id': ['max', list]}).reset_index()
            temp_observation_period.columns = ['person_id', 'observation_period_start_date', 'observation_period_end_date', 'observation_period_id_max', 'observation_period_id_list']
            exploded = temp_observation_period.explode('observation_period_id_list')
            observation_period_mapping = exploded.set_index('observation_period_id_list')['observation_period_id_max'].to_dict()
            observation_period['observation_period_id'] = observation_period['observation_period_id'].map(observation_period_mapping)
            observation_period = observation_period.drop_duplicates()
            observation_period.to_csv(update_output_directory, index=False)
        
            del observation_period, temp_observation_period, exploded
        
        
        # In[ ]:
        
        original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'payer_plan_period_2018.csv')
        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'payer_plan_period_2018.csv')
        print('-------------------------PAYER PLAN PERIOD Transforming...---------------------')
        payer_plan_period = pd.read_csv(original_input_directory)
        payer_plan_period['person_id'] = payer_plan_period['person_id'].astype(str)
        payer_plan_period['payer_plan_period_id'] = payer_plan_period['payer_plan_period_id'].astype(str)
        if person_need_map == True:
            payer_plan_period['person_id'] = payer_plan_period['person_id'].map(person_mapping)
        
        subsets = list(payer_plan_period.columns)
        subsets.remove('payer_plan_period_id')
        if len(payer_plan_period)==len(payer_plan_period.drop_duplicates(subset=subsets)):
            payer_plan_period.to_csv(update_output_directory, index=False)
            del payer_plan_period
        else:
            temp_payer_plan_period = payer_plan_period.groupby(['person_id', 'plan_source_concept_id']).agg({'payer_plan_period_id': ['max', list]}).reset_index()
            temp_payer_plan_period.columns = ['person_id', 'plan_source_concept_id', 'payer_plan_period_id_max', 'payer_plan_period_id_list']
            exploded = temp_payer_plan_period.explode('payer_plan_period_id_list')
            payer_plan_period_mapping = exploded.set_index('payer_plan_period_id_list')['payer_plan_period_id_max'].to_dict()
            payer_plan_period['payer_plan_period_id'] = payer_plan_period['payer_plan_period_id'].map(payer_plan_period_mapping)
            payer_plan_period = payer_plan_period.drop_duplicates()
            payer_plan_period.to_csv(update_output_directory, index=False)
        
            del payer_plan_period, temp_payer_plan_period, exploded


        update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'condition_occurrence_2018.csv')
        print('-------------------------CONDITION OCCURRENCE Transforming...---------------------')
        condition_occurrence = pd.read_csv(update_output_directory)
        condition_occurrence = condition_occurrence.drop_duplicates()
        condition_occurrence.to_csv(update_output_directory, index=False)
        
        # In[ ]:
        
        # original_input_directory = os.path.join(BASE_OUTPUT_DIRECTORY,'procedure_cost_2018.csv')
        # update_output_directory = os.path.join(BASE_UPDATE_OUTPUT_DIRECTORY,'procedure_cost_2018.csv')
        # print('-------------------------PROCEDURE COST Transforming...---------------------')
        # procedure_cost = pd.read_csv(original_input_directory)
        # procedure_cost['procedure_occurrence_id'] = procedure_cost['procedure_occurrence_id'].astype(str)
        # procedure_cost['procedure_cost_id'] = procedure_cost['procedure_cost_id'].astype(str)
        # if procedure_occurrence_need_map == True:
        #     procedure_cost['procedure_occurrence_id'] = procedure_cost['procedure_occurrence_id'].map(procedure_occurrence_mapping)
        
        # if len(procedure_cost)==len(procedure_cost['procedure_occurrence_id'].unique()):
        #     procedure_cost.to_csv(update_output_directory, index=False)
        #     del procedure_cost
        # else:
        #     temp_procedure_cost = procedure_cost.groupby(['procedure_occurrence_id']).agg({'procedure_cost_id': ['max', list]}).reset_index()
        #     temp_procedure_cost.columns = ['procedure_occurrence_id', 'procedure_cost_id_max', 'procedure_cost_id_list']
        #     exploded = temp_procedure_cost.explode('procedure_cost_id_list')
        #     procedure_cost_mapping = exploded.set_index('procedure_cost_id_list')['procedure_cost_id_max'].to_dict()
        #     procedure_cost['procedure_cost_id'] = procedure_cost['procedure_cost_id'].map(procedure_cost_mapping)
        #     procedure_cost = procedure_cost.drop_duplicates()
        #     procedure_cost.to_csv(update_output_directory, index=False)
        
        #     del procedure_cost, temp_procedure_cost, exploded


# In[ ]:

if __name__ == '__main__':
    BASE_OUTPUT_DIRECTORY="E:/EHR-Work/yaoanlee/EHR_Linkage/LDS_ETL_CDM_v1/python_etl/Data/output"
    BASE_UPDATE_OUTPUT_DIRECTORY="E:/EHR-Work/yaoanlee/EHR_Linkage/LDS_ETL_CDM_v1/python_etl/Data/updated_output"
    OutcomeTransform(BASE_OUTPUT_DIRECTORY, BASE_UPDATE_OUTPUT_DIRECTORY)



# In[ ]:





# In[ ]:





# In[ ]:




