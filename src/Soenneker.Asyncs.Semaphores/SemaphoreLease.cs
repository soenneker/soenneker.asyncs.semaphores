using System;
using System.Runtime.CompilerServices;

namespace Soenneker.Asyncs.Semaphores;

/// <summary>
/// Owns one permit acquired from an <see cref="AsyncSemaphore"/>.
/// </summary>
/// <remarks>
/// Dispose the lease to return its permit. Repeated disposal of the same variable is safe. This is a mutable value type;
/// copies represent the same permit and must not be disposed independently.
/// </remarks>
public struct SemaphoreLease : IDisposable
{
    private AsyncSemaphore? _semaphore;

    internal SemaphoreLease(AsyncSemaphore semaphore)
    {
        _semaphore = semaphore;
    }

    /// <summary>
    /// Returns the owned permit to its semaphore. This operation is idempotent.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        AsyncSemaphore? semaphore = _semaphore;

        if (semaphore is null)
            return;

        _semaphore = null;
        semaphore.ReleaseLease();
    }
}
