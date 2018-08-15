######################################
#
#	Rescale API for beamff.exe
#							2018/08/15
######################################
import requests
import re
import json
from collections import OrderedDict
import pprint
from time import sleep
from contextlib import closing

#file upload 1
file_upload = requests.post(
	'https://platform.rescale.jp/api/v2/files/contents/',
	data=None,
#use rb option with binary file
	files={'file': open('beamff.exe','rb')},
	headers={'Authorization': 'Token <API Token>'} 
)

#create dictionary from upload content
file_dict1=json.loads(file_upload.content)
pprint.pprint(file_dict1)
print("\n")

#get file_id from upload content
storage_id=file_dict1['storage']['id']
file_id1=file_dict1['id']

print("storage_id= "+storage_id)
print("file_id1= "+file_id1)
print("\n")

#file upload 2
file_upload = requests.post(
	'https://platform.rescale.jp/api/v2/files/contents/',
	data=None,
	files={'file': open('beam.in')},
	headers={'Authorization': 'Token <API Token>'} 
)

file_dict2=json.loads(file_upload.content)

pprint.pprint(file_dict2)
print("\n")

storage_id=file_dict2['storage']['id']
file_id2=file_dict2['id']

print("storage_id= "+storage_id)
print("file_id2= "+file_id2)
print("\n")

#create job
job_setup = requests.post(
	'https://platform.rescale.jp/api/v2/jobs/',
	json = {
		"name": "BEAM.EXE DOEXXX 000", 
		"jobanalyses": [
			{
				"useRescaleLicense":False, 
				"command": "beamff.exe beam.in beam.out", 
				"analysis": { 
					"code": "user_included_win", 
					"version": "0",
					"name": "Bring Your Own Windows Software",
					"versionName": "Custom" 
					}, 
				"hardware": { 
					"coresPerSlot": 2, 
					"slots": 1, 
					"coreType": "hpc-3"
					}, 
				"inputFiles": [ 
					{
						"id": file_id1
						},
					{
						"id": file_id2
						}
					]
				}
			]
		},
	headers={'Authorization': 'Token <API Token>'} 
)

#create dictionary from job content
job_dict=json.loads(job_setup.content)

pprint.pprint(job_dict)
print("\n")

#get job_id from job content
job_id=job_dict['id']

print("job_id= "+job_id)
print("storage_id= "+storage_id)
print("file_id1= "+file_id1)
print("file_id2= "+file_id2)
print("\n")

#submit job
job_submit=requests.post(
	'https://platform.rescale.jp/api/v2/jobs/'+job_id+'/submit/',
	headers={'Authorization': 'Token <API Token>'} 
)

print(job_submit.content)
print("\n")

#monitor job
job_status=requests.get(
	'https://platform.rescale.jp/api/v2/jobs/'+job_id+'/statuses/',
	headers={'Authorization': 'Token <API Token>'} 
)

job_status_dict=json.loads(job_status.content)
pprint.pprint(job_status_dict)
print("\n")

#seach 'Compleated' from job_status
job_status_comp=str(job_status.content).find('Completed')
print(job_status_comp)

#loop monitoring until 'Completed' appears with 10sec sleep
job_status_comp=-1
while job_status_comp == -1:
	print('not completed')
	sleep(10)
	job_status=requests.get(
		'https://platform.rescale.jp/api/v2/jobs/'+job_id+'/statuses/',
		headers={'Authorization': 'Token <API Token>'} 
	)
	job_status_dict=json.loads(job_status.content)
	pprint.pprint(job_status_dict)
	print("\n")
	job_status_comp=str(job_status.content).find('Completed')
	print(job_status_comp)

sleep(10)
print('Completed')

#get job_result for output file_id
job_result=requests.get(
	'https://platform.rescale.jp/api/v2/jobs/'+job_id+'/files/',
	headers={'Authorization': 'Token <API Token>'}
)
job_result_dict=json.loads(job_result.content)
pprint.pprint(job_result_dict)

job_result_contents=re.sub('"',"",str(job_result.content))
job_result_content_list=re.split('[{}:,]',job_result_contents)
#print("\n")
#print(job_result_content_list)
print("\n")
out_list=[i for i, x in enumerate(job_result_content_list) if x == 'beam.out']
print(out_list)

beamout_id=job_result_content_list[out_list[1]+33]

print(beamout_id)

#download file
response = requests.get(
	'https://platform.rescale.jp/api/v2/files/'+beamout_id+'/contents/',
	headers={'Authorization': 'Token <API Token>'}
)

with open('beam.out', 'wb') as fd:
	for chunk in response.iter_content(chunk_size=100):
		fd.write(chunk)

sleep(10)
print("file downloaded")

