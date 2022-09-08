import neo4j
import neo4j.exceptions


class GdbClient:

    _uri = "neo4j://127.0.0.1:7687"
    _user = "neo4j"
    _password = "123"

    def __enter__(self):
        self.driver = neo4j.GraphDatabase.driver(
            self._uri, auth=(self._user, self._password)
        )
        return self

    # def __enter__(self, uri, user, password):
    #     self.driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
    #     return self

    def __exit__(self, exc_type, exc_value, trace):
        self.driver.close()
        pass

    def _dict_to_str(self, my_dict: dict) -> str:
        mylist = [f"{key}: '{val}'" for key, val in my_dict.items()]
        return "{" + ",".join(mylist) + "}"

    def _tx_run(self, tx, query):
        return [record for record in tx.run(query)]

    def create_node(self, node_type, node_details):
        details = self._dict_to_str(node_details)
        query = f"MERGE (n:{node_type} {details}) RETURN n"
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result

    def create_node_and_relation(
        self,
        from_node_type,
        from_node_details,
        relation_type,
        relation_details,
        to_node_type,
        to_node_details,
    ):

        from_details = self._dict_to_str(from_node_details)
        relationship_details = self._dict_to_str(relation_details)
        to_details = self._dict_to_str(to_node_details)
        query = (
            f"MERGE (n1:{from_node_type} {from_details})"
            f"MERGE (n2:{to_node_type} {to_details})"
            f"MERGE (n1)-[r:{relation_type} {relationship_details}]->(n2)"
            "RETURN n1,n2,r"
        )
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result

    def create_relation(
        self,
        q_from_node_type,
        q_from_node_details,
        relation_type,
        relation_details,
        q_to_node_type,
        q_to_node_details,
    ):
        from_details = self._dict_to_str(q_from_node_details)
        relationship_details = self._dict_to_str(relation_details)
        to_details = self._dict_to_str(q_to_node_details)

        query = (
            f"MATCH (n1:{q_from_node_type} {from_details})"
            f"MATCH (n2:{q_to_node_type} {to_details})"
            f"MERGE (n1)-[r:{relation_type} {relationship_details}]->(n2)"
            "RETURN n1,n2,r"
        )
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result

    def read_node(self, node_type, node_details):
        details = self._dict_to_str(node_details)
        query = f"MATCH (n:{node_type} {details}) RETURN n"
        with self.driver.session() as session:
            result = session.read_transaction(self._tx_run, query)
        return result

    def update_node_details(self, node_type, old_node_details, new_node_details):
        old_details = self._dict_to_str(old_node_details)
        new_details = self._dict_to_str(new_node_details)
        query = f"MATCH (n:{node_type} {old_details}) SET n +={new_details} RETURN n"
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result

    def delete_relation(
        self,
        from_node_type,
        from_node_deetails,
        relation_type,
        relation_details,
        to_node_type,
        to_node_details,
    ):
        from_details = self._dict_to_str(from_node_deetails)
        relationship_details = self._dict_to_str(relation_details)
        to_details = self._dict_to_str(to_node_details)

        query = (
            f"MATCH (n1:{from_node_type} {from_details})-"
            f"[r:{relation_type} {relationship_details}]->"
            f"(n2:{to_node_type} {to_details}) "
            "DELETE r "
            "RETURN n1,n2"
        )
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result

    def delete_node(self, node_type, node_details):
        details = self._dict_to_str(node_details)
        query = f"MATCH (n:{node_type} {details}) " "DETACH DELETE n"
        with self.driver.session() as session:
            result = session.write_transaction(self._tx_run, query)
        return result
