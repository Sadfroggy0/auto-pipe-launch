import pandas as pd
import http.client
import json
import ssl
import time
import collections
import threading

#################GLOBAL SETTINGS#################
file_name = "./Repos.xlsx"
# writer = pd.ExcelWriter("./Repos.xlsx", engine="xlsxwriter")
# worksheet = writer.sheets['suraj1']
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE
queue = collections.deque(maxlen=3)
lock =threading.Lock()
branch_name = "api/sectest_090624"
semaphore = threading.BoundedSemaphore(value=1)
##################################################

##Метод возвращает json-список информации о репозиториях из xlsx 
def getDataFromFile(file):
    raw_data = pd.read_excel(file)
    data = raw_data.to_dict("records")
    return data

# def writeDataToFile(file):
#     raw_data = pd.read_excel(file)
#     data = raw_data.to_dict("records")
#     for i in data:
#         if pd.isnull(i['status']):
#             i['status'] = "pending"
#         else:
#             print(f"it is not null it is {i['status']}")
#     output = pd.DataFrame.from_dict(data)
#     print(output)
#     output.to_excel(file_name)
# writeDataToFile(file_name)

# conn.request("GET", "/api/v4/projects/1736/pipelines?ref=check/proRules&status=success", headers=headers)

def launchRepoPipe(id, start_branch):
    global queue
    global repositories
    conn = http.client.HTTPSConnection("gitlab.host", context=ssl_context, timeout=10 )
    headers = {
        'PRIVATE-TOKEN': '9zFDNtcWsPz6xWCnWeQH',
        'Content-Type': 'application/json'
    }

    commit_data = {
        "branch": str(branch_name),
        "start_branch": str(start_branch),
        "commit_message": "some commit message",
        "actions": [
            {
                "action": "create",
                "file_path": ".gitlab-ci.yml",
                "content":"""stages:
 - sast
 - deps-src
include: 
 - project: "awesome-security/pipelines/sast-pipes"
   file: ".gitlab-ci.yml"
 - project: "awesome-security/pipelines/deps-pipes"
   file: ".gitlab-ci.yml" """
        }
    ]
}
    commit_data_400_error = {
        "branch": str(branch_name),
        "start_branch": str(start_branch),
        "commit_message": "some commit message",
        "actions": [
            {
                "action": "update",
                "file_path": ".gitlab-ci.yml",
                "content":"""stages:
 - sast
 - deps-src
include: 
 - project: "awesome-security/pipelines/sast-pipes"
   file: ".gitlab-ci.yml"
 - project: "awesome-security/pipelines/deps-pipes"
   file: ".gitlab-ci.yml" """
        }
    ]
}
    commit_data_400_error_branch = {
        "branch": str(branch_name),
        "commit_message": "some commit message",
        "actions": [
            {
                "action": "update",
                "file_path": ".gitlab-ci.yml",
                "content":"""stages:
 - sast
 - deps-src
include: 
 - project: "awesome-security/pipelines/sast-pipes"
   file: ".gitlab-ci.yml"
 - project: "awesome-security/pipelines/deps-pipes"
   file: ".gitlab-ci.yml" """
        }
    ]
}
    json_data = json.dumps(commit_data)
    print(f"pushing config file to project w/ id: {id}... ")    
    conn.request("POST", f"/api/v4/projects/{id}/repository/commits", body=json_data, headers=headers)
    response = conn.getresponse()
    conn.close()
    if response.status == 400:
        print("Such file or branch already exeists! Updating the exeisting file on the branch...")
        json_data =  json.dumps(commit_data_400_error)
        conn.request("POST", f"/api/v4/projects/{id}/repository/commits", body=json_data, headers=headers)
        response = conn.getresponse()
        print(response.status)
        conn.close()
        if(response.status == 200 or response.status == 201):
            print("File pushed successfully")
        elif response.status == 400:
            print("Such branch already exists")
            json_data =  json.dumps(commit_data_400_error_branch)
            conn.request("POST", f"/api/v4/projects/{id}/repository/commits", body=json_data, headers=headers)
            response = conn.getresponse()
        else:
            print(f"An error occured while pushing file to repository #{id} : {response}" )
        # response = conn.getresponse().read().decode()
        json_response = json.loads(response.read().decode())
    else: json_response = json.loads(response.read().decode())
    commit_id = json_response.get('id')
    
    while True:
        time.sleep(60)
        print(f"checking status for pipeline of the project w/ id: {id}")
        conn.request("GET", f"/api/v4/projects/{id}/repository/commits/{commit_id}", headers=headers)
        response = conn.getresponse().read().decode()
        print(response)
        try:
            pipeline_status = json.loads(response).get('last_pipeline').get('status')
            if pipeline_status == "failed" or pipeline_status == "canceled" or pipeline_status == "success":
                print(f"pipeline of project with id {id} has {pipeline_status.upper()} status. \n Deleting it from the queue... ")
                lock.acquire()
                # for i in queue:
                #     if i['repo_id'] == id  and len(queue) > 0:
                #         i['status'] = pipeline_status
                if len(queue) > 0:
                    for i in queue:
                        if i['repo_id'] == id  and len(queue) > 0:
                         queue.pop(i)
                print(f" pipeline for project w/ id {id} has been removed from the queue!")
                lock.release()
                break
            else: print(f"pipeline in project #{id} is still running")
        except Exception as e:
            print(f"NO  PIPELINE STATUS for projetc w/ id {id}:\n {e}")
            break
        conn.close()

def queueHandler(item, sem):
    with sem:
        launchRepoPipe(item['repo_id'], item['branch'])
        
#===================================================================================        
def main():
    repositories = getDataFromFile(file_name)
    # queueHandler(repositories=repositories)
    queue = repositories.copy()
    for i in range(len(queue)):
        t = threading.Thread(target=queueHandler, args=(queue[i], semaphore))
        t.start()
    
if __name__ == "__main__":
    main()

        

    


