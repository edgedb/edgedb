from jwcrypto import jwt, jwk
import time
import statistics


def generate_key(key_type):
    if key_type == "ES256":
        return jwk.JWK.generate(kty='EC', crv='P-256')
    elif key_type == "RS256":
        return jwk.JWK.generate(kty='RSA', size=2048)
    elif key_type == "HS256":
        return jwk.JWK.generate(kty='oct', size=256)
    raise ValueError(f"Unsupported key type: {key_type}")


def benchmark_encode(key_type, iterations=100):
    # Generate key outside the loop
    key = generate_key(key_type)

    # Benchmark full encoding process including claims creation
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()

        # Create claims and sign in the timed section
        claims = {"sub": "test"}
        token = jwt.JWT(
            header={"alg": key_type},
            claims=claims
        )
        token.make_signed_token(key)

        end = time.perf_counter_ns()
        times.append(end - start)

    mean = statistics.mean(times) / 1000  # Convert to microseconds
    median = statistics.median(times) / 1000
    return mean, median


def benchmark_signing(key_type, iterations=100):
    # Generate key outside the loop
    key = generate_key(key_type)
    claims = {"sub": "test"}

    # Benchmark signing
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()

        # Signing
        token = jwt.JWT(
            header={"alg": key_type},
            claims=claims
        )
        token.make_signed_token(key)

        end = time.perf_counter_ns()
        times.append(end - start)

    mean = statistics.mean(times) / 1000
    median = statistics.median(times) / 1000
    return mean, median


def benchmark_validation(key_type, iterations=100):
    # Generate key and token outside the loop
    key = generate_key(key_type)
    token = jwt.JWT(
        header={"alg": key_type},
        claims={"sub": "test"}
    )
    token.make_signed_token(key)
    token_string = token.serialize()

    # Benchmark validation
    times = []
    for _ in range(iterations):
        start = time.perf_counter_ns()

        # Validation
        jwt.JWT(jwt=token_string, key=key)

        end = time.perf_counter_ns()
        times.append(end - start)

    mean = statistics.mean(times) / 1000
    median = statistics.median(times) / 1000
    return mean, median


def main():
    key_types = ["ES256", "RS256", "HS256"]
    iterations = 100

    print(f"Running {iterations} iterations for each algorithm")

    print("\nFull encode benchmarks (including claims creation):")
    print(f"{'Algorithm':<10} | {'Mean (µs)':<12} | {'Median (µs)':<12}")
    print("-" * 38)
    for key_type in key_types:
        mean, median = benchmark_encode(key_type, iterations)
        print(f"{key_type:<10} | {mean:12.2f} | {median:12.2f}")

    print("\nSigning benchmarks (pre-created claims):")
    print(f"{'Algorithm':<10} | {'Mean (µs)':<12} | {'Median (µs)':<12}")
    print("-" * 38)
    for key_type in key_types:
        mean, median = benchmark_signing(key_type, iterations)
        print(f"{key_type:<10} | {mean:12.2f} | {median:12.2f}")

    print("\nValidation benchmarks:")
    print(f"{'Algorithm':<10} | {'Mean (µs)':<12} | {'Median (µs)':<12}")
    print("-" * 38)
    for key_type in key_types:
        mean, median = benchmark_validation(key_type, iterations)
        print(f"{key_type:<10} | {mean:12.2f} | {median:12.2f}")


if __name__ == "__main__":
    main()
