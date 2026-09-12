using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.Asyncs.Semaphores.Tests;

public sealed class SemaphoreRegressionTests
{
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(32)]
    public async Task Releasing_an_all_canceled_queue_restores_permits(int count)
    {
        var semaphore = new AsyncSemaphore(0, count);
        using var cancellation = new CancellationTokenSource();
        Task<SemaphoreLease>[] pending = Enumerable.Range(0, count).Select(_ => semaphore.Acquire(cancellation.Token).AsTask()).ToArray();
        cancellation.Cancel();
        foreach (Task<SemaphoreLease> task in pending)
            await Assert.That(async () => await task).Throws<OperationCanceledException>();

        await Task.Run(() => semaphore.Release(count)).WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(semaphore.CurrentCount).IsEqualTo(count);
        var leases = new SemaphoreLease[count];
        for (int i = 0; i < count; i++)
            await Assert.That(semaphore.TryAcquire(out leases[i])).IsTrue();
        foreach (SemaphoreLease lease in leases)
            lease.Dispose();
        await Assert.That(semaphore.CurrentCount).IsEqualTo(count);
    }

    [Test]
    public async Task Multiple_permits_never_grant_one_waiter_twice()
    {
        const int permits = 4;
        var semaphore = new AsyncSemaphore(permits);
        int holders = 0, violations = 0;
        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 5000; i++)
            {
                using SemaphoreLease lease = await semaphore.Acquire();
                if (Interlocked.Increment(ref holders) > permits)
                    Interlocked.Increment(ref violations);
                await Task.Yield();
                Interlocked.Decrement(ref holders);
            }
        }))).WaitAsync(TimeSpan.FromSeconds(15));
        await Assert.That(violations).IsEqualTo(0);
        await Assert.That(semaphore.CurrentCount).IsEqualTo(permits);
    }
}
