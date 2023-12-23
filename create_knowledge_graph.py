import pandas as pd
from py2neo import Graph, Node, Relationship

# Replace the following with your Neo4j database credentials
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USERNAME = "your_username"
NEO4J_PASSWORD = "your_password"

def create_knowledge_graph(data_file):
    # Read the supply chain data from CSV
    df = pd.read_csv(data_file)
    # Connect to the Neo4j database
    
    graph = Graph(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    # Create unique constraints to ensure no duplicates in the graph
    graph.run("CREATE CONSTRAINT ON (p:Product) ASSERT p.ProductID IS UNIQUE")
    graph.run("CREATE CONSTRAINT ON (s:Supplier) ASSERT s.SupplierID IS UNIQUE")
    graph.run("CREATE CONSTRAINT ON (c:Customer) ASSERT c.CustomerID IS UNIQUE")
    # Iterate through each row in the DataFrame and create nodes and relationships
    
    for _, row in df.iterrows():
        product_id = row["ProductID"]
        supplier_id = row["SupplierID"]
        customer_id = row["CustomerID"]
        
        # Create or retrieve nodes for products, suppliers, and customers
        product_node = Node("Product", ProductID=product_id)
        supplier_node = Node("Supplier", SupplierID=supplier_id)
        customer_node = Node("Customer", CustomerID=customer_id)
        graph.merge(product_node, "Product", "ProductID")
        graph.merge(supplier_node, "Supplier", "SupplierID")
        graph.merge(customer_node, "Customer", "CustomerID")
        
        # Create relationships between product and supplier, and product and customer
        supplier_relationship = Relationship(product_node, "SUPPLIER", supplier_node)
        customer_relationship = Relationship(product_node, "CUSTOMER", customer_node)
        graph.create(supplier_relationship)
        graph.create(customer_relationship)
    print("Knowledge graph created successfully!")

if __name__ == "__main__":
    data_file = "supply_chain_data.csv"
    create_knowledge_graph(data_file)
