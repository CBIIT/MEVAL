from bento_mdf import MDFReader
from typing import TypeVar

DataFrame = TypeVar("DataFrame")


class ModelParser:
    """A higher level wrapper of MDFReader class from bento-mdf that offers direct and easy access to model features"""

    def __init__(self, model_file: str, props_file: str, handle: str|None = None):
        """Create a class of ModelParser

        Args:
            model_file (str): file path to the model yaml file
            props_file (str): file path to the properties yaml file
            handle (str | None, optional): model name assigned. Defaults to None.
        """        
        self.model_file = model_file
        self.props_file = props_file
        self.model = MDFReader(self.model_file, self.props_file, handle=handle).model

    def get_node_list(self) -> list[str]:
        """Get the list of node names in the model

        Returns:
            list[str]: list of node names
        """        
        return [node for node in self.model.nodes]

    def get_node_props_list(self, node_name: str) -> list[str]:
        """Get the list of property names for a given node

        Args:
            node_name (str): name of the node
        Returns:
            list[str]: list of property names for the node
        """
        try:
            node_props = [x for x in self.model.nodes[node_name].props]
            return node_props
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' not found in the model.") from e

    def get_node_props_list_required(self, node_name: str) -> list[str]:
        """Get the list of required property names for a given node

        Args:
            node_name (str): name of the node
        Returns:
            list[str]: list of required property names for the node
        """
        try:
            node_obj = self.model.nodes[node_name]
            return [n for n in node_obj.props if node_obj.props[n].is_required]
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' not found in the model.") from e

    def get_prop_attr_dict(self, node_name: str, prop_name: str) -> dict:
        """Get the attributes dictionary for a given property of a node
        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            dict: attributes dictionary of the property
        """
        try:
            if self.model.nodes[node_name].props[prop_name] is not None:
                return self.model.nodes[node_name].props[prop_name].get_attr_dict()
            else:
                raise ValueError(f"Node {node_name} doesn't has a property {prop_name}")
        except KeyError as e:
            raise KeyError(f"Node {node_name} not found in the model.") from e

    def get_node_key_prop(self, node_name: str) -> str:
        """Get the key property name for a given node

        Args:
            node_name (str): name of the node
        Returns:
            str: key property name
        """
        try:
            return self.model.nodes[node_name].get_key_prop().handle
        except KeyError as e:
            raise KeyError(f"Node {node_name} not found in the model.") from e

    def get_prop_type(self, node_name: str, prop_name: str) -> str:
        """Get the data type of a given property of a node

        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            str: data type of the property
        """
        try:
            prop = self.model.nodes[node_name].props[prop_name]
            if prop is not None:
                return prop.value_domain
            else:
                raise ValueError(f"Node '{node_name}' doesn't has a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e

    def get_permissible_values(self, node_name: str, prop_name: str) -> list | None:
        """Get the permissible values of a given property of a node

        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            list: permissible values of a value_set type property
        """
        try:
            node_obj = self.model.nodes[node_name]
            if node_obj.props[prop_name] is not None:
                value_set = node_obj.props[prop_name].values
                # Return can be None is no permissible values defined
                return value_set
            else:
                raise ValueError(f"Node '{node_name}' doesn't have a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e

    def if_prop_required(self, node_name: str, prop_name: str) -> bool:
        """Check if a given property of a node is required

        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            bool: True if the property is required, False otherwise
        """
        try:
            prop = self.model.nodes[node_name].props[prop_name]
            if prop is not None:
                return prop.is_required
            else:
                raise ValueError(f"Node '{node_name}' doesn't have a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e

    def if_prop_key(self, node_name: str, prop_name: str) -> bool:
        """Check if a given property of a node is the key property

        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            bool: True if the property is the key property, False otherwise
        """
        try:
            prop = self.model.nodes[node_name].props[prop_name]
            if prop is not None:
                return prop.is_key
            else:
                raise ValueError(f"Node '{node_name}' doesn't have a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e

    def if_prop_nullable(self, node_name: str, prop_name: str) -> bool:
        """Check if a given property of a node is nullable
        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            bool: True if the property is nullable, False otherwise
        """             
        try:
            prop = self.model.nodes[node_name].props[prop_name]
            if prop is not None:
                return prop.is_nullable
            else:
                raise ValueError(f"Node '{node_name}' doesn't have a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e      

    def if_prop_strict(self, node_name: str, prop_name: str) -> bool:
        """Check if a given property of a node is strict, means strict to the defined list of permissible values

        Args:
            node_name (str): name of the node
            prop_name (str): name of the property
        Returns:
            bool: True if the property is strict, False otherwise
        """
        try:
            prop = self.model.nodes[node_name].props[prop_name]
            if prop is not None:
                return prop.is_strict
            else:
                raise ValueError(f"Node '{node_name}' doesn't have a property '{prop_name}'")
        except KeyError as e:
            raise KeyError(f"Node '{node_name}' or property '{prop_name}' not found in the model.") from e

    def get_parent_nodes(self, node_name) -> list[str]:
        """Get the list of parent node names for a given node

        Args:
            node_name (str): name of the node
        Returns:
            list[str]: list of parent node names
        """
        try:
            edges_list = self.model.edges_by_src(self.model.nodes[node_name])
            return [e.triplet[2] for e in edges_list]
        except KeyError as e:
            raise KeyError(f"Node {node_name} not found in the model.") from e

    def get_child_nodes(self, node_name) -> list[str]:
        """Get the list of child node names for a given node

        Args:
            node_name (str): name of the node
        Returns:
            list[str]: list of child node names
        """
        try:
            edges_list = self.model.edges_by_dst(self.model.nodes[node_name])
            return [e.triplet[1] for e in edges_list]
        except KeyError as e:
            raise KeyError(f"Node {node_name} not found in the model.") from e

    def get_root_node(self) -> str:
        """Get the root node name in the model (the node with no parent nodes).
        We assume there is only one root node in the model. In most cases, it is the study node.

        Returns:
            str: name of the root node
        """
        node_list = self.get_node_list()
        found_root = False
        for node in node_list:
            if self.if_root_node(node_name=node):
                found_root = True
                root_node = node
                break
        if found_root:
            return root_node
        else:
            raise ValueError("No root node found in the model.")

    def if_root_node(self, node_name: str) -> bool:
        """Check if a given node is a root node (no parent nodes)

        Args:
            node_name (str): name of the node
        Returns:
            bool: True if the node is a root node, False otherwise
        """
        parent_nodes = self.get_parent_nodes(node_name)
        return len(parent_nodes) == 0
    
    def if_leaf_node(self, node_name: str) -> bool:
        """Check if a given node is a leaf node (no child nodes)

        Args:
            node_name (str): name of the node
        Returns:
            bool: True if the node is a leaf node, False otherwise
        """        
        child_nodes = self.get_child_nodes(node_name)
        return len(child_nodes) == 0
    
    def get_all_edge_triplets(self) -> list[tuple[str, str, str]]:
        """Get all edge triplets in the model

        Returns:
            list[tuple[str, str, str]]: list of edge triplets (handle, edge_src, edge_dst)
        """
        edges_dict = self.model.edges
        return list(edges_dict.keys())

    def get_edge_multiplicity(self, edge_src: str, edge_dst: str) -> str:
        """Get the multiplicity of a given edge src node to dst node

        Args:
            edge_src (str): source node of the edge
            edge_dst (str): destination node of the edge
        Returns:
            str: multiplicity of the edge, e.g., 'many_to_one', 'one_to_many', etc.
        """
        all_triplets = self.get_all_edge_triplets()
        if_found = False
        for triplet in all_triplets:
            if triplet[1] == edge_src and triplet[2] == edge_dst:
                if_found = True
                edge_multiplicity = self.model.edges[triplet].multiplicity
                break
            else:
                continue
        if if_found:
            return edge_multiplicity
        else:
            raise KeyError(
                f"Edge from '{edge_src}' to '{edge_dst}' not found in the model."
            )

    def get_edge_handle(self, edge_src: str, edge_dst: str) -> str:
        """Get the handle of a given edge src node to dst node

        Args:
            edge_src (str): source node of the edge
            edge_dst (str): destination node of the edge
        Returns:
            str: handle of the edge
        """
        all_triplets = self.get_all_edge_triplets()
        if_found = False
        for triplet in all_triplets:
            if triplet[1] == edge_src and triplet[2] == edge_dst:
                if_found = True
                edge_handle = self.model.edges[triplet].handle
                break
            else:
                continue
        if if_found:
            return edge_handle
        else:
            raise KeyError(
                f"Edge from '{edge_src}' to '{edge_dst}' not found in the model."
            )