
/// cores using fast, hardware-efficient bitwise operations without any memory allocations.
pub fn calculate_shard(key: &str) -> usize {
    // 5381 is an empirical prime number used as the starting point.
    // Starting with this number rapidly reduces hash collisions, preventing
    // all your keys from accidentally piling up on a single core.
    let mut hash: usize = 5381;

    // Loop through every character byte in the key string sequentially
    for byte in key.bytes() {
        // EXPLANATION OF THE BITWISE MATH:
        // 1. `hash << 5` shifts bits left by 5, which mathematically means multiplying by 32.
        // 2. `.wrapping_add(hash)` adds the original value, making it: (hash * 32) + hash = hash * 33.
        //    Multiplying by 33 shuffles the bits thoroughly at each character step.
        // 3. `.wrapping_add(byte as usize)` mixes the current character's ASCII value into the hash.
        // 4. We use `wrapping_add` so if the number grows too large, it wraps around to 0 
        //    instead of crashing the program with an integer overflow error.
        hash = ((hash << 5).wrapping_add(hash)).wrapping_add(byte as usize);
    }

    // Modulo by 4 bounds our final value strictly between 0 and 3.
    // The resulting integer is the exact core number assigned to manage this key.
    hash % 4
}
