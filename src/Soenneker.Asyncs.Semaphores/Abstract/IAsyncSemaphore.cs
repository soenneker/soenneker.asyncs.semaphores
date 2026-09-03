using System;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Semaphores.Abstract;

/// <summary>
/// Represents an in-process semaphore whose permits can be acquired asynchronously and released through an owning lease.
/// </summary>
public interface IAsyncSemaphore
{
    /// <summary>
    /// Gets the number of permits currently available.
    /// </summary>
    int CurrentCount { get; }

    /// <summary>
    /// Gets the maximum number of permits that may be available.
    /// </summary>
    int MaxCount { get; }

    /// <summary>
    /// Acquires one permit asynchronously without cancellation.
    /// </summary>
    /// <returns>An owning lease that releases the permit when disposed.</returns>
    ValueTask<SemaphoreLease> Acquire();

    /// <summary>
    /// Acquires one permit asynchronously with cancellation.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the wait for a permit.</param>
    /// <returns>An owning lease that releases the permit when disposed.</returns>
    /// <exception cref="OperationCanceledException">The cancellation token was canceled before a permit was acquired.</exception>
    ValueTask<SemaphoreLease> Acquire(CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to acquire one permit without waiting.
    /// </summary>
    /// <param name="lease">The owning lease when acquisition succeeds; otherwise, the default lease.</param>
    /// <returns><see langword="true"/> when a permit was acquired; otherwise, <see langword="false"/>.</returns>
    bool TryAcquire(out SemaphoreLease lease);

    /// <summary>
    /// Adds one permit to the semaphore.
    /// </summary>
    /// <returns>The number of available permits before the release.</returns>
    /// <exception cref="SemaphoreFullException">The release would increase the available permits beyond <see cref="MaxCount"/>.</exception>
    int Release();

    /// <summary>
    /// Adds the specified number of permits to the semaphore.
    /// </summary>
    /// <param name="releaseCount">The number of permits to add.</param>
    /// <returns>The number of available permits before the release.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="releaseCount"/> is less than one.</exception>
    /// <exception cref="SemaphoreFullException">The release would increase the available permits beyond <see cref="MaxCount"/>.</exception>
    int Release(int releaseCount);
}
