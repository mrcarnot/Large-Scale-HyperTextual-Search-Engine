#!/usr/bin/env python3
"""
Query Embedder for Semantic Search
Generates embeddings for search queries using pre-trained models

Usage:
    python query_embedder.py "covid symptoms treatment"
    python query_embedder.py --model allenai/scibert_scivocab_uncased "vaccine effectiveness"
    python query_embedder.py --batch queries.txt --output embeddings.csv
"""

import argparse
import sys
import torch
import numpy as np
from transformers import AutoTokenizer, AutoModel
from typing import List, Optional
import csv


class QueryEmbedder:
    """Generate embeddings for search queries"""
    
    def __init__(self, model_name: str = "allenai/scibert_scivocab_uncased",
                 device: Optional[str] = None):
        """
        Initialize the embedder
        
        Args:
            model_name: HuggingFace model name (default: SciBERT for scientific text)
            device: 'cpu', 'cuda', or None (auto-detect)
        """
        print(f"Loading model: {model_name}...", file=sys.stderr)
        
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        
        # Set device
        if device is None:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)
        
        self.model.to(self.device)
        self.model.eval()
        
        print(f"Model loaded on {self.device}", file=sys.stderr)
    
    def embed(self, text: str, normalize: bool = True) -> np.ndarray:
        """
        Generate embedding for a single text
        
        Args:
            text: Input text
            normalize: Whether to L2-normalize the embedding
            
        Returns:
            Embedding vector as numpy array
        """
        # Tokenize
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512,
            padding=True
        )
        
        # Move to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}
        
        # Generate embedding
        with torch.no_grad():
            outputs = self.model(**inputs)
        
        # Mean pooling over tokens
        embedding = outputs.last_hidden_state.mean(dim=1).squeeze()
        
        # Convert to numpy
        embedding = embedding.cpu().numpy()
        
        # Normalize if requested
        if normalize:
            norm = np.linalg.norm(embedding)
            if norm > 1e-8:
                embedding = embedding / norm
        
        return embedding
    
    def embed_batch(self, texts: List[str], normalize: bool = True,
                   batch_size: int = 32) -> np.ndarray:
        """
        Generate embeddings for multiple texts
        
        Args:
            texts: List of input texts
            normalize: Whether to L2-normalize embeddings
            batch_size: Batch size for processing
            
        Returns:
            Array of embeddings, shape (len(texts), embedding_dim)
        """
        all_embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            
            # Tokenize batch
            inputs = self.tokenizer(
                batch,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True
            )
            
            # Move to device
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # Generate embeddings
            with torch.no_grad():
                outputs = self.model(**inputs)
            
            # Mean pooling
            embeddings = outputs.last_hidden_state.mean(dim=1)
            
            # Convert to numpy
            embeddings = embeddings.cpu().numpy()
            
            # Normalize if requested
            if normalize:
                norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
                norms = np.where(norms > 1e-8, norms, 1.0)
                embeddings = embeddings / norms
            
            all_embeddings.append(embeddings)
            
            if (i + batch_size) % 100 == 0:
                print(f"Processed {i + batch_size}/{len(texts)} queries...",
                      file=sys.stderr)
        
        return np.vstack(all_embeddings)


def main():
    parser = argparse.ArgumentParser(
        description="Generate embeddings for search queries"
    )
    
    parser.add_argument(
        "query",
        nargs="*",
        help="Query text (if not using --batch)"
    )
    
    parser.add_argument(
        "--model",
        default="allenai/scibert_scivocab_uncased",
        help="HuggingFace model name (default: SciBERT)"
    )
    
    parser.add_argument(
        "--device",
        choices=["cpu", "cuda"],
        help="Device to use (default: auto-detect)"
    )
    
    parser.add_argument(
        "--batch",
        help="File with one query per line"
    )
    
    parser.add_argument(
        "--output",
        help="Output CSV file (for batch mode)"
    )
    
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Don't L2-normalize embeddings"
    )
    
    parser.add_argument(
        "--format",
        choices=["csv", "vector"],
        default="vector",
        help="Output format (default: vector)"
    )
    
    args = parser.parse_args()
    
    # Validation
    if not args.batch and not args.query:
        parser.error("Either provide query text or use --batch")
    
    if args.batch and args.query:
        parser.error("Cannot use both query text and --batch")
    
    # Initialize embedder
    embedder = QueryEmbedder(model_name=args.model, device=args.device)
    
    normalize = not args.no_normalize
    
    # Single query mode
    if args.query:
        query_text = " ".join(args.query)
        
        print(f"Generating embedding for: '{query_text}'", file=sys.stderr)
        
        embedding = embedder.embed(query_text, normalize=normalize)
        
        if args.format == "csv":
            print("query,embedding")
            print(f'"{query_text}","{",".join(map(str, embedding))}"')
        else:
            # Vector format (comma-separated, no quotes)
            print(",".join(map(str, embedding)))
        
        print(f"Embedding dimension: {len(embedding)}", file=sys.stderr)
        print(f"Norm: {np.linalg.norm(embedding):.6f}", file=sys.stderr)
    
    # Batch mode
    else:
        print(f"Reading queries from: {args.batch}", file=sys.stderr)
        
        with open(args.batch, 'r') as f:
            queries = [line.strip() for line in f if line.strip()]
        
        print(f"Loaded {len(queries)} queries", file=sys.stderr)
        
        embeddings = embedder.embed_batch(queries, normalize=normalize)
        
        print(f"Generated {len(embeddings)} embeddings", file=sys.stderr)
        print(f"Embedding dimension: {embeddings.shape[1]}", file=sys.stderr)
        
        # Write output
        if args.output:
            with open(args.output, 'w', newline='') as f:
                writer = csv.writer(f)
                
                # Header
                writer.writerow(['query'] + [f'dim{i}' for i in range(embeddings.shape[1])])
                
                # Data
                for query, embedding in zip(queries, embeddings):
                    writer.writerow([query] + embedding.tolist())
            
            print(f"Embeddings written to: {args.output}", file=sys.stderr)
        else:
            # Print to stdout in CSV format
            print("query," + ",".join([f'dim{i}' for i in range(embeddings.shape[1])]))
            for query, embedding in zip(queries, embeddings):
                print(f'"{query}",' + ",".join(map(str, embedding)))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
