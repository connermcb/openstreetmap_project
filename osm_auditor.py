# -*- coding: utf-8 -*-
"""
OpenStreetMap Auditor: This module parses and audits different fields 
of OpenStreetMap data. Parses elements labeled as 'node', 'way', 'relation' along
with child elements labeled as 'tag'. Includes functions for auditing tags related
to addresses, population, and amenities.
"""
from collections import defaultdict
import csv
import os
import re
import time
import xml.etree.cElementTree as ET
"""

""" 

class Audit(object):
    
    def __init__(self, file_name):
        """
        Creates defaultdictionaries for tag counts. Compiles regex expressions
        for later matching. 
        
        Args:
            file_name(str): file name with file extension (.osm) to be parsed
            and audited. 
        """
        self.pop_est = {}
        self.file_name = file_name
        self.street_type_regex = re.compile(r'\S+\.?$', re.IGNORECASE)
        self.clean_number = re.compile(r"\['#']\d+")
        self.street_direc_regex = re.compile(r'^\S+\.?', re.IGNORECASE)
        self.street_types = defaultdict(int)
        self.street_direcs = defaultdict(int)
        self.amenities = defaultdict(int)
        self.cities = defaultdict(int)

        
    def get_popul_est(self, f=r"2015_txpopest_place.csv"):
        """ 
        Creates dictionary with city names as keys and list of population figures
        as values. Dictionary referenced for checking accuracy of OSM population
        figures in self.shape_population().
        
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
        reader = csv.reader(open(f))
        for row in reader:
            #row[2] = 2010 US Census figure, row[4] = Texas estimate for 1-1-2016
            self.pop_est[row[1]] = [row[2], row[4]]   
        return self.pop_est

        
    def get_element(self, tags=('node', 'way', 'relation')):
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
    
                
    def get_elem_attribs(self):
        """
        Gets unique values for each type of element. 
        
        Returns:
            None
            
        Prints:
            Dictionary of sets of unique values keyed to their respective element
            type.
        """
        attribs = {'node': set([]), 'way': set([]), 'relation': set([])}
        
        for element in self.get_element():
            for a in element.attrib:
                attribs[element.tag].add(a)
        print(attribs)
        
        
    def get_file_size(self):
        """
        Gets size of OSM file.
        
        Prints:
            File size in megabytes.
        """
        size = os.stat(self.file_name).st_size        
        print("\nOpened OSM file size = {} megabytes".format(round(size/float(1000000), 2)))
    
        
    def get_osm_stats(self):
        """
        Gets count of each of three element types: node, way, relation.
        
        Prints:
            Counts for node, way, relation elements in OSM file. 
        """
        print("\nCounting element tags in file...")
        counts = {'node':0, 'way':0, 'relation':0}
        
        for element in self.get_element():
            counts[element.tag] += 1
        
        for key in counts:
            print("{}s in {} = {}".format(key.capitalize(), self.file_name, counts[key]))
    
            
    def print_sorted_dict(self, d):
        """
        Sort function for ordering and printing items in dictionary.
        
        Args:
            d(dict)
        """
        sorted_keys = sorted(d.keys(), key=lambda s: s.lower())
        for k in sorted_keys:
            v = d[k]
            print('{}:{}'.format(k, v))
        
            
    def get_tag_names(self):
        """
        Gets set of uniqe tag names from child tags in osm file.
        
        Returns:
            tag_names(set): unique tag values.
        """
        tag_names = set([])
        for element in self.get_element():
            if len(element) > 0:
                for child in element:
                    tag_names.add((child.get('k'), child.get('v')))
        return tag_names
     
        
    def audit_street_direc(self):
        """
        Compile set of unique street direciton values by taking last element 
        of split string from 'addr:street' tag.
        
        Prints:
            dictionary of sorted values, where k=last element of str addr:street
            and v=total count for tag name.
        """
        time_start = time.time()
        print("\nProcessing OSM file...")
        for element in self.get_element():
            for tag in element.iter('tag'):
                if tag.get('k') == 'addr:street':
                    street = tag.get('v')
                    t = self.street_direc_regex.search(street)
                    if t:
                        t = t.group()
                        self.street_direcs[t] += 1
        time_end = time.time()
        total_time = round(time_end - time_start, 4)
        print("Data processed in {} secs. \n\nResulting set of tag values for street type: \n".format(total_time))
        self.print_sorted_dict(self.street_direcs)
        
        
    def audit_streets(self):
        """
        Compile set of unique values for child.v where child.k='addr:street'.
        
        Prints:
            Sorted dictionary of unique street name values.
        """
        time_start = time.time()
        count = 0
        streets = defaultdict(int)
        print("\nProcessing OSM file...")
        for element in self.get_element():
            for tag in element.iter('tag'):
                if tag.get('k') == 'addr:street':
                    street = tag.get('v')
                    if street in streets:
                        streets[street] += 1
                    else:
                        streets[street] = 1

        time_end = time.time()
        total_time = round(time_end - time_start, 4)
        print("Data processed in {} secs. \n\nResulting set of tag values for street type: \n".format(total_time))
        for item in streets.items():
            if item[1]<3:
                print(item)
        self.print_sorted_dict(self.street_types)

        
    def audit_cities(self):
        """
        Compile set of unique values for child.v where child.k='addr:city'.
        
        Prints:
            Dictionary key = addr:city.v(str), v = count.
        """
        time_start = time.time()
        print("\nProcessing OSM file...")
        for element in self.get_element():
            for tag in element.iter('tag'):
                if tag.get('k') == 'addr:city':
                    city = tag.get('v') 
                    self.cities[city] += 1
        time_end = time.time()
        total_time = round(time_end - time_start, 4)
        print("Data processed in {} secs. \n\nResulting set of tag values for street type: \n".format(total_time))
        print(dict(self.cities))
        self.print_sorted_dict(self.cities)
   
        
    def audit_amenities(self):
        """
        Compile set of unique values for child.v where child.k='amenity'.
        
        Prints:
            Dictionary key = amenity.v(str), v = count.
        """
        time_start = time.time()
        print("\nProcessing OSM file...")
        for element in self.get_element():
            for tag in element.iter('tag'):
                if tag.get('k') == 'amenity':
                    city = tag.get('v') 
                    self.amenities[city] += 1
        time_end = time.time()
        total_time = round(time_end - time_start, 4)
        print("Data processed in {} secs. \n\nResulting set of tag values for street type: \n".format(total_time))
        self.print_sorted_dict(self.amenities)       

        
    def audit_population(self):
        """
        Check osm population data against Texas Demographic Center estimates for
        2016.
        
        Prints:
            List of lists:[city/town name, osm population, 2016 estimated population]
        """
        time_start = time.time()
        popul_data = []
        print("\nProcessing OSM file...")
        for element in self.get_element():
            name = element.findall("./tag[@k='name']")
            popul = element.findall("./tag[@k='population']")
            if not popul:
                continue
            popul = popul[0].attrib['v']
            if name:
                name = name[0].attrib['v']
                popul_2016 = self.pop_est.get(name, [0, popul])
            popul_data.append([name, popul, popul_2016[1]])
        time_end = time.time() - time_start
        print("\nFile processed in {} secs.".format(time_end))
        return popul_data
                
        
    def audit_tags(self, tag_name):
        """
        Compile set of unique values for given tag name.
        
        Arg:
            tag_name(str): name of child elements to be parsed
            
        Prints:
            Set of unique values for child elements where tag.k=tag_name
        """
        time_start = time.time()
        tag_values = set([])
        print("\nProcessing OSM file...")
        for element in self.get_element():
            for tag in element.iter('tag'):
                name = tag.get('k')
                if tag.attrib['k'] == tag_name:
                    name = tag.get('v')
                    if name:
                        name = name.split(' ')[-1]
                    tag_values.add(name)


        time_end = time.time()
        total_time = round(time_end - time_start, 4)
        print("Data processed in {} secs. \n\nResulting set of values for tag_name = {}: \n".format(total_time, tag_name))
        print(tag_values)
 
     
        
        
if __name__ == '__main__':
        
    # Initialize Austin OSM audit object
    a = Audit(r'austin_subset.osm')

    # Audit file
    a.audit_streets()
    a.audit_amenities()
    a.audit_streets()
    a.audit_population
    a.get_file_size()
    a.get_osm_stats()
    a.audit_tags('addr:postcode')