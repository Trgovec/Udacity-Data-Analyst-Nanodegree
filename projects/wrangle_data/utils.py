#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
After auditing is complete the next step is to prepare the data to be inserted into a SQL database.
To do so you will parse the elements in the OSM XML file, transforming them from document format to
tabular format, thus making it possible to write to .csv files.  These csv files can then easily be
imported to a SQL database as tables.

The process for this transformation is as follows:
- Use iterparse to iteratively step through each top level element in the XML
- Shape each element into several data structures using a custom function
- Utilize a schema and validation library to ensure the transformed data is in the correct format
- Write each data structure to the appropriate .csv files

We've already provided the code needed to load the data, perform iterative parsing and write the
output to csv files.

Your task is to complete the shape_element function that will transform each
element into the correct format.

To make this process easier we've already defined a schema (see
the schema.py file in the last code tab) for the .csv files and the eventual tables. Using the
cerberus library we can validate the output against this schema to ensure it is correct.

## Shape Element Function
The function should take as input an iterparse Element object and return a dictionary.

### If the element top level tag is "node":
The dictionary returned should have the format {"node": .., "node_tags": ...}

The "node" field should hold a dictionary of the following top level node attributes:
- id
- user
- uid
- version
- lat
- lon
- timestamp
- changeset
All other attributes can be ignored

The "node_tags" field should hold a list of dictionaries, one per secondary tag. Secondary tags are
child tags of node which have the tag name/type: "tag". Each dictionary should have the following
fields from the secondary tag attributes:
- id: the top level node id attribute value
- key: the full tag "k" attribute value if no colon is present or the characters after the colon if one is.
- value: the tag "v" attribute value
- type: either the characters before the colon in the tag "k" value or "regular" if a colon
        is not present.

Additionally,

- if the tag "k" value contains problematic characters, the tag should be ignored
- if the tag "k" value contains a ":" the characters before the ":" should be set as the tag type
  and characters after the ":" should be set as the tag key
- if there are additional ":" in the "k" value they and they should be ignored and kept as part of
  the tag key. For example:

  <tag k="addr:street:name" v="Lincoln"/>
  should be turned into
  {'id': 12345, 'key': 'street:name', 'value': 'Lincoln', 'type': 'addr'}

- If a node has no secondary tags then the "node_tags" field should just contain an empty list.

The final return value for a "node" element should look something like:

{'node': {'id': 757860928,
          'user': 'uboot',
          'uid': 26299,
       'version': '2',
          'lat': 41.9747374,
          'lon': -87.6920102,
          'timestamp': '2010-07-22T16:16:51Z',
      'changeset': 5288876},
 'node_tags': [{'id': 757860928,
                'key': 'amenity',
                'value': 'fast_food',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'cuisine',
                'value': 'sausage',
                'type': 'regular'},
               {'id': 757860928,
                'key': 'name',
                'value': "Shelly's Tasty Freeze",
                'type': 'regular'}]}

### If the element top level tag is "way":
The dictionary should have the format {"way": ..., "way_tags": ..., "way_nodes": ...}

The "way" field should hold a dictionary of the following top level way attributes:
- id
-  user
- uid
- version
- timestamp
- changeset

All other attributes can be ignored

The "way_tags" field should again hold a list of dictionaries, following the exact same rules as
for "node_tags".

Additionally, the dictionary should have a field "way_nodes". "way_nodes" should hold a list of
dictionaries, one for each nd child tag.  Each dictionary should have the fields:
- id: the top level element (way) id
- node_id: the ref attribute value of the nd tag
- position: the index starting at 0 of the nd tag i.e. what order the nd tag appears within
            the way element

The final return value for a "way" element should look something like:

{'way': {'id': 209809850,
         'user': 'chicago-buildings',
         'uid': 674454,
         'version': '1',
         'timestamp': '2013-03-13T15:58:04Z',
         'changeset': 15353317},
 'way_nodes': [{'id': 209809850, 'node_id': 2199822281, 'position': 0},
               {'id': 209809850, 'node_id': 2199822390, 'position': 1},
               {'id': 209809850, 'node_id': 2199822392, 'position': 2},
               {'id': 209809850, 'node_id': 2199822369, 'position': 3},
               {'id': 209809850, 'node_id': 2199822370, 'position': 4},
               {'id': 209809850, 'node_id': 2199822284, 'position': 5},
               {'id': 209809850, 'node_id': 2199822281, 'position': 6}],
 'way_tags': [{'id': 209809850,
               'key': 'housenumber',
               'type': 'addr',
               'value': '1412'},
              {'id': 209809850,
               'key': 'street',
               'type': 'addr',
               'value': 'West Lexington St.'},
              {'id': 209809850,
               'key': 'street:name',
               'type': 'addr',
               'value': 'Lexington'},
              {'id': '209809850',
               'key': 'street:prefix',
               'type': 'addr',
               'value': 'West'},
              {'id': 209809850,
               'key': 'street:type',
               'type': 'addr',
               'value': 'Street'},
              {'id': 209809850,
               'key': 'building',
               'type': 'regular',
               'value': 'yes'},
              {'id': 209809850,
               'key': 'levels',
               'type': 'building',
               'value': '1'},
              {'id': 209809850,
               'key': 'building_id',
               'type': 'chicago',
               'value': '366409'}]}
"""
import os
import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET

import cerberus

import schema

DATA_DIR = 'data'
OSM_PATH = "beijing_china_sample.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# ================================================== #
#                 Audit and Clean                    #
# ================================================== #

### for phone
PHONE_CC = '86'
PHONE_AC = '10'
PHONE_LANDLINE = 8

phone_re = re.compile(r'\D*(?P<cc>%s)?\D*(?P<ac>%s|%s)?\D*(?P<lc>\d{8,})' % (PHONE_AC, r'0'+PHONE_AC, PHONE_AC))

def format_phonenumber(pn):
    '''
    Return formatted phone number, marked with 'N/A' if it's invalid.
    '''
    result = phone_re.search(pn)
    # invalid
    if not result:
        return 'N/A ' + pn
    lc = result.group('lc')
    nlc = len(lc)
    # invalid
    if nlc < PHONE_LANDLINE:
        return 'N/A ' + pn
    # cell phone, no area code
    elif nlc > PHONE_LANDLINE:
        return '+{}-{}'.format(PHONE_CC, lc)
    # landline, add area code
    else:
        return '+{}-{}{}'.format(PHONE_CC, PHONE_AC, lc)

def update_phone(phone):
    '''
    Update phone entry with uniformed format.
    phone can be multiple values concatenated with ';' like `68716285;62555813`
    '''
    return ';'.join([format_phonenumber(pn) for pn in phone.split(';')])

### for name and address
mapping = {'Rd':'Road',
          'St':'Street',
          'Str':'Street',
          'E':'East',
          'W':'West',
          'N':'North',
          'S':'South',
          'Ave':'Avenue',
          'Dr':'Drive',
          'Bldg':'Building',
          'Pky':'Parkway'}

# expand abbrevation to inlcude '.'
mapping.update({str(k)+'.':v for k,v in mapping.items()})
# match 'N.' first, then 'N'
addr_keys = sorted(mapping.keys(), reverse=True)
addr_re = re.compile(r'\b(?P<abbre>' + r'|'.join([re.escape(k) for k in addr_keys]) + r')(?:\s+|$)')

def is_abbre_addr(addr):
    return addr_re.match(addr.title()) != None

def update_address(addr):
    # x.group returns 'N. ' for sub, need to mannually add space and then strip
    new_addr = addr_re.sub(lambda x: mapping[x.group('abbre')] + ' ', addr.title())
    return new_addr.strip()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))

def audit_element(element):
    '''
    Audit and clean element for errors of fixme, phone formats, address and name formats.
    '''
    tag_key = element.tag + '_tags'
    tags = element[tag_key]
    r = []
    for t in tags:
        k = t['key']
        # audit fixme
        if k == 'FIXME' or k == 'fixme':
            continue
        # audit phone
        if k == 'phone':
            t['value'] = update_phone(t['value'])
        # audit address
        if (t['type'] == 'addr' and k == 'street') or k == 'en':
            t['value'] = update_address(t['value'])
        r.append(t)
    return r


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE
    element_id = element.get('id')
    if element.tag == 'node':
        attrib_dict = {'node': NODE_FIELDS,
                       'tag': ['k', 'v']}
        attribs = get_element_attribs(element, attrib_dict)
        tags = get_tags(element_id, attribs['tag'], default_tag_type)
        return {'node': attribs['node'][0],
                'node_tags': tags}
    elif element.tag == 'way':
        attrib_dict = {'way': WAY_FIELDS,
                       'nd': ['ref'],
                       'tag': ['k', 'v']}
        attribs = get_element_attribs(element, attrib_dict)
        return {'way': attribs['way'][0],
                'way_nodes': [{'id': element_id, 'node_id': nd['ref'], 'position':i}
                              for i, nd in enumerate(attribs['nd'])],
                'way_tags': get_tags(element_id, attribs['tag'], default_tag_type)}

# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element_attribs(element, attrib_dict):
    '''
    Return attribute dicts of element, both k and v of attrib_dict should be the raw strings in html.
    For example, for node, attrib_dict = {'node': node_fields, 'tag': ['k', 'v']}
    '''
    item_tags = attrib_dict.keys()
    result = {tag:[] for tag in item_tags}
    for item in element.iter():
        item_tag = item.tag
        attribs = {k:item.attrib[k] for k in attrib_dict[item_tag]}
        result[item_tag].append(attribs)
    return result

def get_tags(element_id, raw_tags, default_type):
    '''
    Return modified tags ['id', 'key', 'value', 'type'] for a list of original tags attribs ['k', 'v']
    '''
    tags = []
    for tag in raw_tags:
        k = tag['k']
        if not PROBLEMCHARS.search(k):
            attribs = {'id': element_id, 'value': tag['v']}
            if ':' in k:
                attribs['type'], attribs['key'] = k.split(':', 1)
            else:
                attribs['type'], attribs['key'] = default_type, k
            tags.append(attribs)
    return tags

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def get_sample(osm_file, sample_file, k=10):
    '''
    Take a systematic sample of elements from your original OSM region.
    Try changing the value of k so that your resulting SAMPLE_FILE ends up at different sizes.
    When starting out, try using a larger k, then move on to an intermediate k before processing your whole dataset.
    '''
    with open(sample_file, 'wb') as output:
        output.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        output.write('<osm>\n  ')

        # Write every kth top level element
        for i, element in enumerate(get_element(osm_file)):
            if i % k == 0:
                output.write(ET.tostring(element, encoding='utf-8'))

        output.write('</osm>')


def process_map(file_in, validate=False, audit=False):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)
                if audit is True:
                    el = audit_element(el)
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])


'''
if __name__ == '__main__':
    # Note: Validation is ~ 10X slower. For the project consider using a small
    # sample of the map when validating.
    if not os.path.isdir(DATA_DIR):
        os.mkdir(DATA_DIR)
    os.chdir(DATA_DIR)
    process_map(OSM_PATH, validate=True)
'''
