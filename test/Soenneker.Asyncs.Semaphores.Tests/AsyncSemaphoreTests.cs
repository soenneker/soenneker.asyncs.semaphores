using System;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Semaphores.Tests;

public sealed class AsyncSemaphoreTests
{
    [Test]
    public async Task Acquire_should_wait_until_a_lease_is_disposed()
    {
        var semaphore = new AsyncSemaphore(1);
        using SemaphoreLease first = await semaphore.Acquire();

        ValueTask<SemaphoreLease> pending = semaphore.Acquire();

        await Assert.That(pending.IsCompleted).IsFalse();
        first.Dispose();

        using SemaphoreLease second = await pending;
        await Assert.That(semaphore.CurrentCount).IsEqualTo(0);
    }

    [Test]
    public async Task Lease_disposal_should_be_idempotent()
    {
        var semaphore = new AsyncSemaphore(1);
        SemaphoreLease lease = await semaphore.Acquire();

        lease.Dispose();
        lease.Dispose();

        await Assert.That(semaphore.CurrentCount).IsEqualTo(1);
    }

    [Test]
    public async Task TryAcquire_should_not_wait()
    {
        var semaphore = new AsyncSemaphore(1);

        await Assert.That(semaphore.TryAcquire(out SemaphoreLease lease)).IsTrue();
        await Assert.That(semaphore.TryAcquire(out _)).IsFalse();

        lease.Dispose();
        await Assert.That(semaphore.CurrentCount).IsEqualTo(1);
    }

    [Test]
    public async Task Acquire_should_observe_cancellation()
    {
        var semaphore = new AsyncSemaphore(0, 1);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        async Task Acquire() => _ = await semaphore.Acquire(cancellation.Token);

        await Assert.That(Acquire).Throws<OperationCanceledException>();
    }

    [Test]
    public async Task Release_should_make_a_permit_available()
    {
        var semaphore = new AsyncSemaphore(0, 2);
        ValueTask<SemaphoreLease> pending = semaphore.Acquire();

        int previousCount = semaphore.Release();
        using SemaphoreLease lease = await pending;

        await Assert.That(previousCount).IsEqualTo(0);
        await Assert.That(semaphore.CurrentCount).IsEqualTo(0);
        await Assert.That(semaphore.MaxCount).IsEqualTo(2);
    }

    [Test]
    public async Task Concurrent_releases_should_wake_every_waiter()
    {
        const int permitCount = 64;
        var semaphore = new AsyncSemaphore(0, permitCount);
        Task<SemaphoreLease>[] acquisitions = Enumerable.Range(0, permitCount)
                                                        .Select(_ => semaphore.Acquire().AsTask())
                                                        .ToArray();

        Task[] releases = Enumerable.Range(0, permitCount)
                                    .Select(_ => Task.Run(() => semaphore.Release()))
                                    .ToArray();

        await Task.WhenAll(releases);
        SemaphoreLease[] leases = await Task.WhenAll(acquisitions).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(semaphore.CurrentCount).IsEqualTo(0);

        await Task.WhenAll(leases.Select(lease => Task.Run(lease.Dispose)));
        await Assert.That(semaphore.CurrentCount).IsEqualTo(permitCount);
    }

    [Test]
    public async Task Canceled_waiter_should_not_consume_a_permit()
    {
        var semaphore = new AsyncSemaphore(0, 1);
        using var cancellation = new CancellationTokenSource();
        Task<SemaphoreLease> canceled = semaphore.Acquire(cancellation.Token).AsTask();
        Task<SemaphoreLease> acquisition = semaphore.Acquire().AsTask();

        cancellation.Cancel();

        await Assert.That(async () => await canceled).Throws<OperationCanceledException>();

        semaphore.Release();
        SemaphoreLease acquiredLease;

        try
        {
            acquiredLease = await acquisition.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (TimeoutException exception)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
            long state = (long)typeof(AsyncSemaphore).GetField("_state", flags)!.GetValue(semaphore)!;
            int count = unchecked((int)state);
            int pendingHandoffs = (int)((ulong)state >> 32);
            throw new TimeoutException($"Count={count}, PendingHandoffs={pendingHandoffs}", exception);
        }

        using SemaphoreLease lease = acquiredLease;

        await Assert.That(semaphore.CurrentCount).IsEqualTo(0);
    }

    [Test]
    public async Task Cancellation_and_release_races_should_preserve_the_permit()
    {
        var semaphore = new AsyncSemaphore(1);

        for (var i = 0; i < 1_000; i++)
        {
            SemaphoreLease holder = await semaphore.Acquire();
            using var cancellation = new CancellationTokenSource();
            ValueTask<SemaphoreLease> pending = semaphore.Acquire(cancellation.Token);

            await Task.WhenAll(Task.Run(cancellation.Cancel), Task.Run(holder.Dispose));

            try
            {
                SemaphoreLease lease = await pending;
                lease.Dispose();
            }
            catch (OperationCanceledException)
            {
            }

            await Assert.That(semaphore.CurrentCount).IsEqualTo(1);
        }
    }

    [Test]
    public async Task Batch_release_should_skip_canceled_waiters_without_losing_handoffs()
    {
        const int activeCount = 128;
        var semaphore = new AsyncSemaphore(0, activeCount);
        var cancellations = new CancellationTokenSource[activeCount];
        var canceled = new Task<SemaphoreLease>[activeCount];
        var active = new Task<SemaphoreLease>[activeCount];

        for (var i = 0; i < activeCount; i++)
        {
            cancellations[i] = new CancellationTokenSource();
            canceled[i] = semaphore.Acquire(cancellations[i].Token).AsTask();
            active[i] = semaphore.Acquire().AsTask();
        }

        await Task.WhenAll(cancellations.Select(source => Task.Run(source.Cancel)));
        semaphore.Release(activeCount);

        SemaphoreLease[] leases = await Task.WhenAll(active).WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(semaphore.CurrentCount).IsEqualTo(0);

        foreach (Task<SemaphoreLease> task in canceled)
            await Assert.That(async () => await task).Throws<OperationCanceledException>();

        foreach (SemaphoreLease lease in leases)
            lease.Dispose();

        foreach (CancellationTokenSource source in cancellations)
            source.Dispose();

        await Assert.That(semaphore.CurrentCount).IsEqualTo(activeCount);
    }

    [Test]
    public async Task Parallel_handoffs_should_not_lose_permits()
    {
        var semaphore = new AsyncSemaphore(1);

        async Task Worker()
        {
            for (var i = 0; i < 100_000; i++)
            {
                using SemaphoreLease lease = await semaphore.Acquire();
                await Task.Yield();
            }
        }

        await Task.WhenAll(Worker(), Worker(), Worker(), Worker()).WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(semaphore.CurrentCount).IsEqualTo(1);
    }

    [Test]
    public async Task Repeated_parallel_batches_should_not_lose_permits()
    {
        var semaphore = new AsyncSemaphore(1);
        var completedBatches = 0;

        async Task Worker()
        {
            using SemaphoreLease lease = await semaphore.Acquire();
            await Task.Yield();
        }

        async Task RunBatches()
        {
            for (var i = 0; i < 10_000; i++)
            {
                await Task.WhenAll(Worker(), Worker(), Worker(), Worker());
                Volatile.Write(ref completedBatches, i + 1);
            }
        }

        try
        {
            await RunBatches().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException exception)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
            long state = (long)typeof(AsyncSemaphore).GetField("_state", flags)!.GetValue(semaphore)!;
            int count = unchecked((int)state);
            int pendingHandoffs = (int)((ulong)state >> 32);
            throw new TimeoutException($"Batches={completedBatches}, Count={count}, PendingHandoffs={pendingHandoffs}", exception);
        }

        await Assert.That(semaphore.CurrentCount).IsEqualTo(1);
    }
}
