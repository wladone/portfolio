# Python Fallback Scripts

⚠️ **DEPRECATED**: These Python scripts are kept for reference and as fallbacks only.

## Status

This directory contains the original Python implementations that have been converted to Scala. The Python versions are maintained for:

- **Reference**: Understanding the original logic and requirements
- **Fallback**: When Scala compilation or runtime issues occur
- **Compatibility**: Environments where Scala/SBT is not available

## 🚫 Primary Implementation

**Use the Scala versions instead!** Located in `src/main/scala/com/example/logstream/`

The Scala implementations are:
- More performant (JVM optimization)
- Better integrated with the Spark ecosystem
- Type-safe and maintainable
- Actively maintained

## Available Scripts

| Python Script | Scala Equivalent | Purpose |
|---------------|------------------|---------|
| `generate_logs.py` | `LogGenerator` | Generate synthetic Apache log data |
| `test_logparser.py` | `LogParserTest` | Test log parsing functionality |
| `benchmark_performance.py` | `PerformanceBenchmark` | Performance testing and benchmarking |

## Usage (Fallback Only)

If Scala is not working, you can use these Python scripts:

```bash
# Generate test data
python python-fallback/generate_logs.py --output input/access.log --lines 1000

# Test parsing
python python-fallback/test_logparser.py

# Run benchmarks
python python-fallback/benchmark_performance.py --test scalability
```

## Migration Notes

- All Python scripts have been successfully converted to Scala
- The Scala versions maintain the same CLI interfaces where applicable
- Additional features like the web dashboard are Scala-only
- Python dependencies are no longer required for the main project

## When to Use Python Versions

- Scala compilation issues
- Development environments without JVM/SBT
- Quick prototyping before Scala implementation
- Debugging when Scala output is unclear