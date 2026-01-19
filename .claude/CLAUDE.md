# dfdb-client-clj Development Guidelines

# Build System
# Use Clojure CLI (deps.edn) for builds and tests: `clj -M:test`

# Testing
# ALWAYS run tests after making code changes: `clj -M:test`
# Tests require a running dfdb-go server on localhost:8081
# Use ./scripts/start-server.sh to start the server before running tests

# REPL Development
# Start REPL with: `clj -M:dev`
# Always use `:reload` when requiring namespaces to pick up changes
# Create temporary Clojure files in a sandbox subdirectory if needed

# Clojure Style
# Avoid atoms to hold state where possible (prefer reduce and other accumulators)
# Use parameterized queries instead of string formatting when passing values

# Don't revert to simple approaches automatically without prompting the user!
