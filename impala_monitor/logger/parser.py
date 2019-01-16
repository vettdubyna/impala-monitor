import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


class Query(object):
    def __init__(self, query: dict):
        for key in query:
            self.__dict__[key] = query[key]

    def __getattr__(self, item):
        if item not in self.__dict__:
            raise ValueError("Item {} does not exists".format(item))

        return self.__dict__[item]

    def __setattr__(self, key, value):
        self.__dict__[key] = value

    def to_dict(self):
        new_dict = {}
        for key in self.__dict__:
            value = self.__dict__[key]
            if isinstance(value, datetime):
                value = value.strftime('%Y-%m-%d %H:%M:%S.%f')
            if isinstance(value, timedelta):
                value = value.total_seconds()

            new_dict[key] = value
        #print(new_dict)
        return new_dict


class Converter(object):
    @staticmethod
    def convert(value: str, convert_to: str) -> int:
        #print("++ Value = " + value)
        #print("+++ Original unit = " + str(value[len(value)-2:len(value)]))
        #print("++++ Original value = " + value[0:len(value)-2])
        original_unit = str(value[len(value)-2:len(value)])
        original_value = float(value[0:len(value)-2])
        if convert_to not in ['KB', 'GB', 'MB', 'TB']:
            raise ValueError('Unit {} not valid'.format(original_unit))

        if original_unit not in ['KB', 'GB', 'MB', 'TB']:
            raise ValueError('Unit {} not valid'.format(original_unit))

        if original_unit == convert_to:
            return original_value

        if original_unit == 'GB' and convert_to == 'MB':
            return round(original_value * 1000)
        elif original_unit == 'MB' and convert_to == 'GB':
            return original_value / 1000
        elif original_unit == 'KB' and convert_to == 'MB':
            return original_value / 1000
        elif original_unit == 'TB' and convert_to == 'MB':
            return original_value / 1000000


class ImpalaQueryLogParser(object):
    def __init__(self, html: str):
        self.soup = BeautifulSoup(html, 'html.parser')

    @property
    def queries(self) -> list:
        """
        From the HTML it should get the list of queries, with some metadata as:
        - Query ID
        - Number of fetched rows
        - Query
        :return: dict
        """
        tables = self.soup.findAll('table')
        #print(tables)
        # In our case we know it's the third table
        rows = tables[2].findAll('tr')
        queries = []

        for row in rows[1:len(rows)]:
            cells = row.findAll("td")
            query_type = cells[3].get_text()
            query_state = cells[8].get_text()
            #print("query type: " + query_type + "; Query state: " + query_state)
            if query_type not in ['QUERY','DDL','DML'] or not query_state in ['FINISHED', 'EXCEPTION']:
                continue

            start_time = datetime.strptime(
                cells[4].get_text(), "%Y-%m-%d %H:%M:%S.%f000"
            )

            end_time = datetime.strptime(
                cells[5].get_text(), "%Y-%m-%d %H:%M:%S.%f000"
            )

            execution_time = end_time - start_time

            query_id = self.extract_query_id(
                cells[11].find('a', href=True).get('href')
            )

            queries.append(Query({
                'query': cells[2].get_text(),
                'query_type': query_type,
                'state': query_state,
                'fetched_rows': int(cells[9].get_text()),
                'user': cells[0].get_text(),
                'start_time': start_time,
                'end_time': end_time,
                'execution_time': execution_time,
                'query_id': query_id,
                'timestamp': int(start_time.timestamp()),
            }))
            #print("+++++++++++++++++++++++++++")
            #print('query: ' + cells[2].get_text()[0:10])
            #print('query_type: ' + query_type)
            #print('state: ' + query_state)
            #print('fetched_rows: ' + cells[9].get_text())
            #print('user: ' + cells[0].get_text())
            #print('start_time: ' + str(start_time))
            #print('end_time: ' + str(end_time))
            #print('execution_time: ' + str(execution_time))
            #print('query_id: ' + query_id)
            #print('timestamp: ' + str(start_time.timestamp()))
            #print("---------------------------\n")
        return queries

    def extract_profile(self, query: Query) -> Query:
        profile = self.soup.findAll('div', {'class': 'container'})[1].find(
            'pre').get_text()
       
        #Retreiving true query type
        query_type_profile = re.search('Query Type: ([QUERYDDLDML]+)', profile)
        #aprint(query_type_profile.group(1))
        query.query_type = query_type_profile.group(1)
        
        memory_allocated_matches = re.search('- PerHostPeakMemUsage: ([0-9\. GBMBKB]+)', profile)
        #print(str(memory_allocated_matches.group(1)))
        if memory_allocated_matches:
            query.memory_allocated = Converter.convert(
                memory_allocated_matches.group(1).replace(" ", ""), 'MB'
            )
        else:
            query.memory_allocated = 0

        vcores_allocated_matches = re.search('VCores=([0-9]+)', profile)

        if vcores_allocated_matches:
            query.vcores_allocated = vcores_allocated_matches.group(1)
        else:
            query.vcores_allocated = 0

        if query.state == 'EXCEPTION':
            exception_message_matches = re.search(
                "Query Status: ([a-zA-Z \:\/_\-0-9\.]+)", profile
            )

            if exception_message_matches:
                query.exception_message = exception_message_matches.group(1)

        if query.state == 'FINISHED':
            query = self.parse_exec_summary(query, profile)

        return query

    def parse_exec_summary(self, query: Query, profile: str) -> Query:
        exec_summary_match = re.search(
            'ExecSummary(.*)([a-z\sA-Z#\.\-0-9\:_\(\),|]+)Query Timeline',
            profile
        )

        if not exec_summary_match:
            return query

        exec_summary = exec_summary_match.group(2)

        if exec_summary:
            query.exec_summary = exec_summary

        return query

    @staticmethod
    def extract_query_id(link: str) -> str:
        """
        Given a link it extracts the query id
        :param link:
        :return:
        """
        regex = 'query_id=([a-z0-9\:]+)'

        matches = re.search(regex, link)

        if not matches:
            return None

        return matches.group(1)
