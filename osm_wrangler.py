# -*- coding: utf-8 -*-
"""
This module parses and cleans OpenStreetMap data files. Cleaning is limited to 
specific tag values ('addr:street', 'addr:city', 'addr:postcode', 'population').

The module writes parsed and cleaned parsed data to csv files, and also includes
method for writing csv to SQL database for later query.

The module also writes corrected data to txt files for optional assessement and
further auditing.
"""

import csv
import os
import pandas as pd
import re
import requests
import sqlite3
import time
import xml.etree.cElementTree as ET

class Wrangler(object):
    """
    Data parsing and cleaning class for OpenStreetMap data file for Austin, Texas
    """
    
    def __init__(self, file_name=None):
        """
        Args:
            file_name(xml): OSM file for processing
        """
        self.file_name = file_name  
        # Dictionary for stats on cleaned tags
        self.review_correct_counts = {'streets':[0, 0], 'cities':[0, 0],
                                      'zipcodes':[0, 0], 'population':[0, 0],
                                      }
        # Text files for optional later auditing and cleaning assessment
        self.cleaned_streets = open(r'cleaned_streets.txt', 'w')
        self.cleaned_cities = open(r'cleaned_cities.txt', 'w')
        self.cleaned_zipcodes = open(r'cleaned_zipcodes.txt', 'w')
        self.cleaned_population = open(r'cleaned_population.txt', 'w') 
        # List of itinialized csv files
        self.initialized = []
        # Dictionary for 2016 Texas population estimates assigned in get_popul_est()
        self.pop_est = {}
        
        
    def get_element(self, tags=('node')):
        """
        Element iterator for parsing and counting nodes. Function relies on 
        ElementTree mondule.
        Function is borrowed from lessons.
        
        Args:
            tags(tuple(str)): Names of elements to be parsed.
            
        Yield:
            Single xml element and its child tags.
        """
        context = ET.iterparse(self.file_name, events=('start', 'end'))
        __, root = next(context)
        for event, elem in context:
            if event == 'end' and elem.tag in tags:
                yield elem
                root.clear()
                
    def reset_data_files(self):
        """
         Deletes csv files for eventual re-initialization in process_data(). 
         Deletes all files in list self.initialized from local directory. 
        """
        for each in ['node_attribs.csv',  
                     'tag_attribs.csv',
                     'cleaned_streets.txt',
                     'cleaned_cities.txt',
                     'cleaned_zipcodes.txt',
                     'cleaned_population.txt',
                     'p3_osm']:
            try:
                os.remove(each)
            except:
                continue
        # Initialize csv file for node attributes
        self.nodes_csv_file = open('node_attribs.csv', 'w')
        node_fieldnames = ['lat', 'uid', 'lon', 'timestamp', 'user',
                      'id', 'changeset', 'version']
        self.node_writer = csv.DictWriter(self.nodes_csv_file, fieldnames=node_fieldnames)
        self.node_writer.writerow({v:v for v in node_fieldnames})
        # Initialize csv file for tag attributes
        self.tags_csv_file = open('tag_attribs.csv', 'w')
        tag_fieldnames = ['id', 'k', 'v']
        self.tag_writer = csv.DictWriter(self.tags_csv_file, fieldnames=tag_fieldnames)      
        self.tag_writer.writerow({v:v for v in tag_fieldnames})
            
    def get_popul_est(self, file=r"2015_txpopest_place.csv"):
        """ 
        Creates dictionary with city names as keys and list of population figures
        as values. Dictionary referenced for checking accuracy of OSM population
        figures in shape_population().
        
        Args:
            file(csv): Texas Demographics Center population data and estimates
            
        Returns:
            None
            
            Adds str(city_name):[str(2010 Census data), str(2016 population estimate)]
            to class initialized dictionary, self.pop_est.
            
        Examples:
            'Fredericksburg': ['10530', '11075']
            'Pflugerville': ['46936', '56313']
        """
        reader = csv.reader(open(file))
        for row in reader:
            #row[2] = 2010 US Census figure, row[4] = Texas estimate for 1-1-2016
            self.pop_est[row[1]] = [row[2], row[4]] 
  
            
    def get_zcode(self, lat, lon):
        """
        Function called from shape_zipcode() in the case postal code value is
        None or can't be corrected. Function uses google maps api to fetch postal 
        code using latitude and longitude data.
        
        Args:
            lat(float): latitude associated with OSM node
            lon(float): longitude associated with OSM node
            
        Returns:
            int: five-digit postal code associated with given coordinates
             or
            None: if no postal code returned from api request
        """
        if type(lat) != str or type(lon) != str:
            return None
        lat = float(lat)
        lon = float(lon)
        base = "http://maps.googleapis.com/maps/api/geocode/json?"
        params = "latlng={},{}&sensor=False".format(lat, lon)
        url = "{base}{params}".format(base=base, params=params)
        response = requests.get(url)
        try:
            address_components = response.json()['results'][0]['address_components']
        except:
            print(response.json()['error_message'])
            return None
        zcode = [compon['long_name'] for compon in address_components if compon['types'][0] == 'postal_code'][0]
        if zcode:
            return int(zcode)
        return None

        
    def shape_zipcode(self, zcode, lat, lon):
        """
        Function corrects postal code to standard five-digit format.
        
        Args:
            zcode(str): uncorrected value for OSM 'addr:postcode' tag
            lat(float): latitude associated with currect OSM node
            lon(float): longitude associated with current OSM node
            
        Returns:
            str: corrected and standardized five-digit postal code
            None: if postal code unretrievable or uncorrectable
        """
        zcode = zcode.encode('ascii', 'ignore').decode('ascii')
        new_zcode = re.findall(r'(\d{5})', zcode)
        if new_zcode and new_zcode[0][0] == '7':
            new_zcode = new_zcode[0]
            self.cleaned_zipcodes.write("Original entry: {}, Cleaned entry: {}\n".format(zcode, new_zcode))
            return new_zcode
        else:
            new_zcode = self.get_zcode(lat, lon)
            self.cleaned_zipcodes.write("Original entry: {}, Cleaned entry: {}\n".format(zcode, new_zcode))
            return new_zcode
        return None 

     
    def shape_streetname(self, street):
        """
        Function corrects value for OSM 'addr:street' tags. Removes numbers
        prefixed with pound sign. Corrects and standardizes both street direction
        and street type designations.
        
        Args:
            street(str): uncorrected value for OSM 'addr:street' tag
            
        Returns:
            str: corrected and standardized street name
            
        Examples:
            The 'addr:street' value '15000 W. William Cannon Blvd #300' is corrected
            to 'West William Cannon Boulevard'
            
        """
        orig_street = street # Assign original variable to variable for later correction check
        street = re.sub("#\w+", "", street).strip() # Remove number prefixed with pound sign
        street_elements = [e.strip('.') for e in street.split(' ')] 
        if street_elements[0].isdigit():
            street_elements.pop(0)
        # Check for and correct street direction designation
        directions = ['North', 'South', 'East', 'West']
        direc_d = {'N':'North', 'S':'South', 'E':'East', 'W':'West'}
        for i in [0, len(street_elements)-1]:
            if street_elements[i] in direc_d:
                street_elements.insert(0, direc_d[street_elements.pop(i)])
                break
            if street_elements[i].capitalize() in directions:
                street_elements.insert(0, street_elements.pop(i).capitalize())
                break
        # Check for and correct street type designation
        street_types_d = {'blvd':'Boulevard', 'st':'Street', 'ave':'Avenue', 'hwy':'Highway',
                          'IH':'Interstate Highway', 'cir':'Circle', 'ct':'Court', 'cv':'Cove',
                          'dr':'Drive', 'ln':'Lane', 'rd':'Road', 'pkwy':'Parkway', 
                          }
        for i in range(len(street_elements)):
            if street_elements[i].lower() in street_types_d:
                street_type = street_elements[i].lower()
                if street_type == 'hwy': # Correct unique case of highway designation
                    street_elements[i] = street_types_d[street_elements[i].lower()]
                    break
                else:
                    street_elements.insert(len(street_elements)-1, street_types_d[street_elements.pop(i).lower()])
        new_street = " ".join([e.capitalize() for e in street_elements])
        # Check if street name was changed in any way
        if street != new_street:
            # Write changes to txt file to facilitate possible further auditing
            self.cleaned_streets.write('Original entry: "{}", Cleaned entry: "{}"\n'.format(orig_street, new_street))
        return new_street

        
    def shape_city(self, city):
        """
        Function corrects city name trimming extraneous characters and removing
        information such as state better suited to other OSM tags.
        
        Args:
            city(str): uncorrected value of OSM 'addr:city' tag.
            
        Returns:
            str: city name with spelling and capitalization corrected, without
            state information and unnecessary punctation 
        """
        cities = ['Austin', 'Georgetown', 'Creedmoor', 'Dripping Springs', 
                  'Taylor', 'West Lake Hills', 'Cedar Park', 'Elgin', 'Lakeway',
                  'Bastrop', 'Buda','Cedar Creek', 'Manchaca',
                  'Spicewood', 'Dale', 'Kyle', 'Leander', 'San Marcos', 'Manor',
                  'Lago Vista', 'Maxwell', 'Sunset Valley', 'Del Valle', 
                  'Smithville', 'Lost Pines', 'Webberville', 'Bee Cave', 
                  'Spicewood', 'Elgin', 'Wimberley', 'Pflugerville', 'Round Rock',
                  'Hutto', 'Jonestown', 'Barton Creek', 'Driftwood', 'Liberty Hill', 
                  'Leander']
        
        for name in cities:
            temp = name.replace(' ', '')
            city = "".join(city.split())
            if re.findall(temp, city, re.IGNORECASE):
                self.cleaned_cities.write("Original entry: {}, Cleaned entry: {}\n".format(city, name))
                return name
            else:
                continue       
        return None

        
    def shape_population(self, name, popul):
        """
        Update population figures to most recent gov estimates
        Update OSM settlement type based on revised population figures
        Input: list of len = 7, 
            [OSM node id, city name, OSM settlement type, T/F for settlement type change,
            OSM population, revised population (init None), OSM pop source]
        Output: None
        """
        osm_pop = int(popul)
        pop_2016 = self.pop_est.get('name', osm_pop)
        if pop_2016 != osm_pop:
            self.cleaned_population.write("{} - OSM population: {}, Revised or kept population: {}\n".format(name, osm_pop, pop_2016))
        return pop_2016
   
        
    def process_data(self):
        """
        Function gets OSM element from get_element generator and writes parent
        attributes to csv file using pandas dataframes. Tag (child) elements 
        are assigned to shape functions according to value of tag['k']. Corrected
        data returned from shape functions re-assigned and tag attributes written
        to csv file using pandas dataframes.
        
        Args:
            Calls iterator self.get_element()
            
        Returns:
            None
            
            Writes attributes of element and its subelements labeled 'tag' to csv
            files.
        """
        time_start = time.time()
        exemption_count = 0
        count_tags = 0
        count_tags_cleaned = 0
        print("Start time: {}".format(time.asctime()))
        print("Processing OSM file...")
        self.reset_data_files()  # Reset data files to avoid data duplication
        for element in self.get_element():
            element_id = element.attrib['id']
            lat = element.get('lat')
            lon = element.get('lon')  
            try:
                self.node_writer.writerow({k:v for k, v in element.attrib.items()})#.encode('utf-8')
            except:
                exemption_count += 1
                continue
            if len(element) == 0:
                continue     
            for child in element.findall("./tag"):
                child.attrib['id'] = element_id
                k = child.get('k')
                v = child.get('v')
                new = None
                count_tags += 1
                if k == 'addr:postcode':
                    new = self.shape_zipcode(v, lat, lon)
                    child.attrib['v'] = new
                elif k == 'addr:city':
                    new = self.shape_city(v)
                    child.attrib['v'] = new
                elif k == 'addr:street':
                    new = self.shape_streetname(v)
                    child.attrib['v'] = new
                elif k == 'population':
                    city_name = element.findall("./tag[@k='name']")[0].attrib['v']
                    new = self.shape_population(city_name, v)
                    child.attrib['v'] = new
                if new != v:
                    count_tags_cleaned += 1
                try:
                    self.tag_writer.writerow({k:v for k, v in child.attrib.items()})
                except:
                    exemption_count += 1
                    continue
        self.nodes_csv_file.close()
        self.tags_csv_file.close()
        self.cleaned_cities.close()
        self.cleaned_population.close()
        self.cleaned_streets.close()
        self.cleaned_zipcodes.close()
        time_end = time.time()
        total_time = round(time_end - time_start, 1)
        print("Data processed in {} secs.".format(total_time))
        print("Number of tags reviewed: {}".format(count_tags))
        print("Number of tags corrected: {}".format(count_tags_cleaned))
        print("Number of tags and nodes exempted: {}".format(exemption_count))

        
    def csv_to_sql(self):
        """
        Relies on pandas module to read csv data into dataframe and subsequently
        write data into sql tables stored in database p3_osm.sqlite3. 
        """
        start = time.time()
        print("Writing csv file to sql database...")
        connect = sqlite3.connect(r'p3_osm')
        cur = connect.cursor()
        cur.execute('DROP TABLE IF EXISTS tag_attribs;')
        cur.execute('CREATE TABLE tag_attribs (id, k, v);') 
        
        with open(r'tag_attribs.csv', 'r') as tag_f:
            dr = csv.DictReader(tag_f)
            to_sql = [(i['id'], i['k'], i['v']) for i in dr] 
        cur.executemany("INSERT INTO tag_attribs (id, k, v) VALUES (?, ?, ?);", to_sql)
        connect.commit()
        tag_f.close()
        
        
        cur.execute('DROP TABLE IF EXISTS node_attribs;')
        cur.execute('CREATE TABLE node_attribs (lat, uid, lon, timestamp, user, id, changeset, version);') 
        
        with open(r'node_attribs.csv', 'r') as tag_f:
            dr = csv.DictReader(tag_f)
            to_sql = [(i['lat'], i['uid'], i['lon'], i['timestamp'], i['user'],
                       i['id'], i['changeset'], i['version']) for i in dr] 
        cur.executemany("INSERT INTO node_attribs (lat, uid, lon, timestamp, user,\
                         id, changeset, version) VALUES (?,?,?,?,?,?,?,?);", to_sql)
        connect.commit()
        tag_f.close()  
        connect.close()
        print("File written to sql database in {} secs.".format(time.time()-start))


    def query_sql(self, query, n):
        """
        Connects to project database 'p3_osm.db', initializes cursor, and
        executes 'query' results of which are converted to dataframe and printed.
        
        Args:
            query(str): sqlite3 query command
            n(int): number of rows to be printed by query_results_df.head(n)
            
        Returns:
            None
            
        Prints:
            Results of query outputted as dataframe
        """
        p3_db = sqlite3.connect(r'p3_osm')
        curs = p3_db.cursor()
        curs.execute(query)
        query_results = curs.fetchall()
        if query_results:
            names = [each[0] for each in curs.description]
        else:
            return
        query_results_df = pd.DataFrame(query_results)
        query_results_df.columns = names
        print(query_results_df.head(n))
        print()
        curs.close()
        

if __name__ == '__main__':
        
    # Initialize Austin OSM wrangler object
    a = Wrangler(r'austin_subset.osm')
    # Parse and clean
    a.get_popul_est()
    a.process_data()
    a.csv_to_sql()
    # Query
    a.query_sql("CREATE TABLE IF NOT EXISTS zipcodes AS SELECT * FROM tag_attribs WHERE k='addr:postcode';", 20)
    a.query_sql("SELECT tag_attribs.k, tag_attribs.v, COUNT(tag_attribs.v) AS cuisine_count\
                          FROM tag_attribs\
                          WHERE tag_attribs.k = 'cuisine'\
                          GROUP BY tag_attribs.v ORDER BY cuisine_count DESC;", 15)
    a.query_sql("SELECT zipcodes.v, COUNT(tag_attribs.v) AS restaurant_count\
                          FROM tag_attribs, zipcodes\
                          ON tag_attribs.id = zipcodes.id\
                          WHERE tag_attribs.v = 'restaurant'\
                          GROUP BY zipcodes.v ORDER BY restaurant_count DESC;", 15)