"""
Generate a graph that uses UUIDs as vertex IDs.
Output vertex and edge data in either parquet or text file.
"""

import argparse
import pandas as pd
from faker import Faker
import random
import concurrent.futures
import os

def parse_args():
    parser = argparse.ArgumentParser(description='Generate a UUID graph.')
    # n for the number of vertices to generate
    parser.add_argument('-n', type=int, dest='num_vertices', default=10000,
                        help='Number of vertices to generate')
    # m for the number of edges to generate
    parser.add_argument('-d', type=int, dest='degree', default=10,
                        help='Graph degree')

    # o for the output file prefix
    parser.add_argument('-o', type=str, dest='output_file_prefix',
                        default='./data',
                        help='Output file prefix')

    # b for batch size
    parser.add_argument('-b', type=int, dest='batch_size', default=1_000_000,
                        help='Batch size for parallel generation')

    return parser.parse_args()
def generate_vertices(num_vertices):
    fake = Faker()
    return [fake.uuid4() for _ in range(num_vertices)]

def generate_edges(vertices, degree):
    edges = []
    for vertex in vertices:
        for _ in range(degree):
            edges.append((vertex, random.choice(vertices)))
    return edges

def save_to_parquet(df, filename):
    df.to_parquet(filename)

def save_to_text(df, filename):
    df.to_csv(filename, sep=' ', header=False, index=False)

def save_graph(vertices, edges, output_file_prefix):
    vertex_df = pd.DataFrame({'id': vertices})
    edge_df = pd.DataFrame(edges, columns=['src', 'dst'])

    save_to_parquet(vertex_df, f'{output_file_prefix}-vertices.parquet')
    save_to_parquet(edge_df, f'{output_file_prefix}-edges.parquet')

    save_to_text(vertex_df, f'{output_file_prefix}-vertices.txt')
    save_to_text(edge_df, f'{output_file_prefix}-edges.txt')

if __name__ == "__main__":
    args = parse_args()
    print(args)

    vertices = generate_vertices(args.num_vertices)
    edges = generate_edges(vertices, args.degree)

    save_graph(vertices, edges, args.output_file_prefix)
    print(f"Data saved to {args.output_file_prefix}-*.[parquet/txt]")