from dbclient import *
from datetime import datetime, date
import re


class SQLAnalyticsClient(ClustersClient):

    def get_scheduled_queries(self):
        all_queries = self.get('/preview/sql/queries/admin').get('results', [])
        scheduled_queries = list(filter(lambda x: x.get('schedule') is not None, all_queries))
        return scheduled_queries

    def delete_scheduled_queries(self, query_list):
        # admins don't have access to modify the scheduled queries, so we del them
        total_deleted = 0
        for query_details in query_list:
            qid = query_details.get('id')
            print(query_details)
            api = f'/preview/sql/queries/{qid}'
            resp = self.post(api, query_details)
            if resp.get('http_status_code') == 200:
                total_deleted += 1
            print(resp)
        return total_deleted

    @staticmethod
    def has_keep_until_tags(cinfo):
        keep_until_tags = ["keepuntil", "keep_until"]
        custom_tags = cinfo.get("tags", {}).get("custom_tags", '')
        if custom_tags:
            for tag in custom_tags:
                if (tag['key'].lower() in keep_until_tags):
                    date_str = tag['value'].lower()
                    date_str = re.sub('[/\-]', '-', date_str)
                    try:
                        expiry_dt = datetime.strptime(date_str, '%m-%d-%Y').date()
                    except:
                        return "Expired"  # not formatted right. Expire.
                    if expiry_dt >= date.today():
                        return "Stop"  # not expired
                    else:
                        return "Expired"  # expired
        return "False"

    def get_sql_endpoints_list(self, alive=True):
        """ Returns an array of json objects for the endpoints. Grab the cluster_name or cluster_id """
        endpoint_list = self.get("/sql/endpoints/").get('endpoints', [])
        if endpoint_list:
            if alive:
                running = list(filter(lambda x: x['state'] == "RUNNING", endpoint_list))
                for x in running:
                    print(x['name'] + ' : ' + x['id'])
                return running
        return endpoint_list

    def get_endpoints_to_terminate(self):
        cluster_list = self.get_sql_endpoints_list()

        if cluster_list:
            # kill list array
            terminate_cluster_list = []
            for cluster in cluster_list:
                co = dict()
                co['cluster_name'] = cluster['name']
                co['creator_user_name'] = cluster['creator_name']
                co['cluster_id'] = cluster['id']
                co['autotermination_minutes'] = cluster['auto_stop_mins']

                co['cluster_details'] = {'min_num_clusters': cluster['min_num_clusters'],
                                         'max_num_clusters': cluster['max_num_clusters'],
                                         'cluster_size': cluster['cluster_size']}

                keep1 = SQLAnalyticsClient.has_keep_alive_tags(cluster)
                keep2 = SQLAnalyticsClient.has_keep_until_tags(cluster)
                co['keep_alive'] = keep1  # False means no keepalive flag so stop
                co['keep_until'] = keep2  # Expire or Stop
                if keep1 == True and keep2 == "Expired":  # keepalive and keepuntil expired. Expiry takes precedence
                    terminate_cluster_list.append(co)
                elif keep1 == True and keep2 == "Stop":  # keepalive and keepuntil Stop. keepalive takes precedence
                    pass
                elif keep1 == True and keep2 == "False":  # keepalive and no keepuntil.
                    pass
                else:  # no keep_alive. keep_until rules kick in.
                    terminate_cluster_list.append(co)

            return terminate_cluster_list
        return []

    def stop_endpoint(self, cid=None):
        """ Stop the Endpoint with id """
        if cid:
            url = f"/sql/endpoints/{cid}/stop"
            resp = self.post(url)
            pprint_j(resp)

    def del_endpoint(self, cid=None):
        """ Kill the endpoint with id """
        if cid:
            url = f"/sql/endpoints/{cid}"
            resp = self.delete(url)
            pprint_j(resp)
