#!/usr/bin/env python3
"""
Generate Postman collection from OpenAPI spec.
"""

import sys
from pathlib import Path

import httpx
import typer
from pydantic import BaseModel


class PostmanRequest(BaseModel):
    method: str
    header: list[dict[str, str]]
    url: dict[str, str]
    description: str | None


class PostmanItem(BaseModel):
    name: str
    request: PostmanRequest
    response: list = []


class PostmanItemGroup(BaseModel):
    name: str
    item: list[PostmanItem]


class PostmanCollection(BaseModel):
    info: dict
    item: list[PostmanItemGroup]
    variable: list[dict] = []


async def fetch_openapi_spec(url: str) -> dict:
    """Fetch OpenAPI spec from running API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()


def convert_path_to_postman(path: str, method: str, spec: dict) -> PostmanItem:
    """Convert OpenAPI path to Postman request."""
    operation = spec["paths"][path][method]

    # Build URL with path parameters
    url = {
        "raw": f"{{baseUrl}}{path}",
        "host": ["{{baseUrl}}"],
        "path": path.strip("/").split("/"),
    }

    # Add headers
    headers = [
        {"key": "Accept", "value": "application/json"},
        {"key": "Content-Type", "value": "application/json"},
    ]
    if operation.get("security"):
        headers.append({"key": "Authorization", "value": "Bearer {{authToken}}"})

    return PostmanItem(
        name=operation.get("summary", path),
        request=PostmanRequest(
            method=method.upper(),
            header=headers,
            url=url,
            description=operation.get("description"),
        ),
    )


def generate_collection(spec: dict) -> PostmanCollection:
    """Generate Postman collection from OpenAPI spec."""
    # Group endpoints by first path segment
    groups: dict[str, list[PostmanItem]] = {}

    for path, methods in spec["paths"].items():
        group = path.strip("/").split("/")[0].title()
        if group not in groups:
            groups[group] = []

        for method in methods:
            item = convert_path_to_postman(path, method, spec)
            groups[group].append(item)

    # Create collection
    return PostmanCollection(
        info={
            "name": "E-commerce Analytics API",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
        },
        item=[
            PostmanItemGroup(name=group, item=items)
            for group, items in sorted(groups.items())
        ],
        variable=[
            {"key": "baseUrl", "value": "http://localhost:8000"},
            {"key": "authToken", "value": "YOUR_JWT_TOKEN"},
        ],
    )


def main(
    output: Path = typer.Option(
        "api/postman_collection.json", help="Output path for Postman collection"
    ),
    api_url: str = typer.Option(
        "http://localhost:8000/openapi.json", help="URL to fetch OpenAPI spec from"
    ),
) -> None:
    """Generate Postman collection from OpenAPI spec."""
    try:
        # Fetch spec
        spec = httpx.get(api_url).json()

        # Generate collection
        collection = generate_collection(spec)

        # Write output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(collection.json(indent=2))

        print(f"Generated Postman collection at {output}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
