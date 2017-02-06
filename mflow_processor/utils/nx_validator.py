from collections import OrderedDict
from xml.etree import ElementTree
from h5py import Dataset, Group

NX_CLASS_ATTRIBUTE_NAME = "NX_class"


class NXSchemaValidator(object):
    def __init__(self, groups, datasets):
        self.groups = groups
        self.datasets = datasets

        self.required_groups = dict((group_name, definition) for group_name, definition
                                    in self.groups.items() if definition["required"])
        self.required_datasets = dict((dataset_name, definition) for dataset_name, definition
                                      in self.datasets.items() if definition["required"])

    def validate_h5_file(self, file_handle):
        errors = ""

        # Validate group requirements.
        for group_name, definition in self.required_groups.items():
            existing_group = file_handle.get(group_name)
            # There is an element with the correct name in the file.
            if not existing_group:
                errors += "Group '%s' missing in file.\n" % group_name
            # The element is a group.
            elif not isinstance(existing_group, Group):
                errors += "The object named '%s' is not a group.\n" % group_name
            # The group has the class attribute.
            elif NX_CLASS_ATTRIBUTE_NAME not in existing_group.attrs:
                errors += "The group '%s' is missing the 'NX_class' attribute.\n" % group_name
            # The group class attribute is correct.
            elif not existing_group.attrs[NX_CLASS_ATTRIBUTE_NAME] == definition["type"]:
                errors += "The group '%s' has the wrong 'NX_class' attribute.\n" % group_name

        # Validate dataset requirements.
        for dataset_name, definition in self.required_datasets.items():
            existing_dataset = file_handle.get(dataset_name)
            # There is an element with the correct name in the file.
            if not existing_dataset:
                errors += "Dataset '%s' missing in file.\n" % dataset_name
            # The element is a dataset.
            elif not isinstance(existing_dataset, Dataset):
                errors += "The object named '%s' is not a dataset.\n" % dataset_name

        if errors:
            raise ValueError(errors)

    def create_required_groups(self, file_handle):
        for group_name, definition in self.required_groups.items():
            group = file_handle.require_group(group_name)
            group.attrs[NX_CLASS_ATTRIBUTE_NAME] = definition["type"]

    def get_required_group_attributes(self):

        h5_group_attributes = OrderedDict(("%s:%s" % (path, NX_CLASS_ATTRIBUTE_NAME), definition["type"])
                                          for path, definition in sorted(self.required_groups.items()))

        return h5_group_attributes


def parse_nx_definition(file_name):
    """
    Parse the provided NX XML file. Return the dictionary of groups and datasets.
    :param file_name: Input NX XML schema.
    :return: groups (dictionary), datasets (dictionary)
    """
    root = ElementTree.parse(file_name).getroot()
    schema_name = root.tag[:root.tag.index("}") + 1]

    tag_group = schema_name + "group"
    tag_dataset = schema_name + "field"
    tag_doc = schema_name + "doc"
    tag_attribute = schema_name + "attribute"
    tag_dimensions = schema_name + "dimensions"

    groups = {}
    datasets = {}

    def is_required(element):
        # No minOccurs attribute means that the group is required.
        return element.attrib.get("minOccurs", "1") != "0"

    def strip_doc_whitespace(input_doc):
        return input_doc.replace("\t", "").replace("\n", " ").strip()

    def process_attribute(attribute, dataset_path):
        attribute_name = attribute.attrib["name"]

        if not datasets[dataset_path].get("attributes"):
            datasets[dataset_path]["attributes"] = []

        datasets[dataset_path]["attributes"].append(attribute_name)

    def process_dataset(dataset, group_path):
        dataset_type = dataset.attrib.get("type", None)
        dataset_path = "%s/%s" % (group_path, dataset.attrib["name"])
        dataset_required = is_required(dataset)

        datasets[dataset_path] = {"type": dataset_type,
                                  "required": dataset_required}

        for node in dataset:
            # Dataset documentation.
            if node.tag == tag_doc:
                datasets[dataset_path]["doc"] = strip_doc_whitespace(node.text)
            # Dataset attribute.
            elif node.tag == tag_attribute:
                process_attribute(node, dataset_path)
            # Dataset dimensions.
            elif node.tag == tag_dimensions:
                datasets[dataset_path]["rank"] = int(node.attrib["rank"])
            else:
                ValueError("Node tag '%s' not expected for dataset." % node.tag)

    def process_group(group, group_path=""):
        group_type = group.attrib["type"]
        group_name = group.attrib.get("name") or group_type[2:]
        group_path += "/%s" % group_name
        group_required = is_required(group)
        # The first 2 characters of the type are always 'NX',
        # and they should be excluded from the group name.
        groups[group_path] = {"attributes": {"NX_class": group_type},
                              "required": group_required}

        for node in group:
            # Group in a group.
            if node.tag == tag_group:
                process_group(node, group_path)
            # Dataset in a group.
            elif node.tag == tag_dataset:
                process_dataset(node, group_path)
            # Group documentation.
            elif node.tag == tag_doc:
                groups[group_path]["doc"] = strip_doc_whitespace(node.text)
            else:
                ValueError("Node tag '%s' not expected for group." % node.tag)

    # We are interested only in the group element in the document root.
    process_group(root.find(schema_name + "group"))

    return {"groups": OrderedDict(sorted(groups.items())),
            "datasets": OrderedDict(sorted(datasets.items()))}