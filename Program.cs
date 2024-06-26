// See https://aka.ms/new-console-template for more information
using System;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Transactions.Config;
using Couchbase.Transactions;
using Couchbase.Transactions.Error;
using System.Text.Json;
using Couchbase.KeyValue;
using System.Reflection.Metadata;
using Google.Api;
using System.Reflection;
using System.Security.Cryptography;
using System.Xml.Linq;
using System.Diagnostics;
using System.Numerics;

Console.WriteLine("Hello, World!");


await new StartUsing().Main(args[0], args[1], args[2], args[3], args[4]);
//await new StartUsing().BulkInsertInTxn();
class StartUsing
{

    private static Random random = new Random();

    public static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }
    public async Task Main(string host, string username, string password, string totalS, string chunkSizeS)
    {
        var total = Int32.Parse(totalS);
        var chunkSize = Int32.Parse(chunkSizeS);

        // Initialize the Couchbase cluster
        // var options = new ClusterOptions().WithCredentials("test", "Pwd12345!");
        var options = new ClusterOptions().WithCredentials(username, password);
      // var cluster = await Cluster.ConnectAsync("couchbases://cb.oafqjrqaclzjpn68.cloud.couchbase.com", options).ConfigureAwait(false);
       var cluster = await Cluster.ConnectAsync(host, options).ConfigureAwait(false);
        var bucket = await cluster.BucketAsync("test").ConfigureAwait(false);
        var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);
        var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);

        // Create the single Transactions object
        var _transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create()
            .DurabilityLevel(DurabilityLevel.None)
        .ExpirationTime(TimeSpan.FromHours(1))
        .Build());

        var documento = new
        {
            _id = "667bfaddb0463c180d804cc9",
            index = 0,
            guid = "5225fdb9-b9c4-4887-8220-7a5eca255531",
            isActive = true,
            balance = "$1,022.47",
            picture = "http=//placehold.it/32x32",
            age = 39,
            body = RandomString(270000),
            eyeColor = "brown",
            name = "Sherri Burke",
            gender = "female",
            company = "ZILLANET",
            email = "sherriburke@zillanet.com"
        };

        try
       {



            //// Define a delegate that prints and returns the system tick count
            //Func<object, int> action = (object ctx) =>
            //{
            //    int i = (int)obj;

            //    // Make each thread sleep a different time in order to return a different tick count
            //    Thread.Sleep(i * 100);

            //    // The tasks that receive an argument between 2 and 5 throw exceptions
            //    if (2 <= i && i <= 5)
            //    {
            //        throw new InvalidOperationException("SIMULATED EXCEPTION");
            //    }

            //    int tickCount = Environment.TickCount;
            //    Console.WriteLine("Task={0}, i={1}, TickCount={2}, Thread={3}", Task.CurrentId, i, tickCount, Thread.CurrentThread.ManagedThreadId);

            //    return tickCount;
            //};

            //// Construct started tasks
            //for (int i = 0; i < 10; i++)
            //{
            //    int index = i;
            //    tasks.Add(Task<int>.Factory.StartNew(action, index));
            //}

            //try
            //{
            //    // Wait for all the tasks to finish.
            //    Task.WaitAll(tasks.ToArray());

            //    // We should never get to this point
            //    Console.WriteLine("WaitAll() has not thrown exceptions. THIS WAS NOT EXPECTED.");
            //}
            //catch (AggregateException e)
            //{
            //    Console.WriteLine("\nThe following exceptions have been thrown by WaitAll(): (THIS WAS EXPECTED)");
            //    for (int j = 0; j < e.InnerExceptions.Count; j++)
            //    {
            //        Console.WriteLine("\n-------------------------------------------------\n{0}", e.InnerExceptions[j].ToString());
            //    }
            //}


            async void operate(AttemptContext ctx, int index)
            {
                for (int i = 0; i < chunkSize; i++)
                {
                    var opt = await ctx.GetOptionalAsync(_collection, (index * chunkSize + i).ToString()).ConfigureAwait(false);
                    if (opt == null)
                        await ctx.InsertAsync(_collection, (index * chunkSize + i).ToString(), documento).ConfigureAwait(false);
                    else
                        await ctx.ReplaceAsync(opt, documento).ConfigureAwait(false);
                }

            };

            var tasks = new List<Task>();
 

            var watch = Stopwatch.StartNew();


            var result = await _transactions.RunAsync(async (ctx) => 
           {
               for (int i = 0; i < total / chunkSize; i++)
               {
                   tasks.Add(Task.Factory.StartNew(() => operate(ctx, i)));
               }
               Task.WaitAll(tasks.ToArray());
               //await Parallel.ForEachAsync(Enumerable.Range(0, total/chunkSize), async (index, token) =>
               //{
               //    await operate(ctx, index).ConfigureAwait(false);
               //    Console.Clear();
               //    Console.Write($"Staged {(index + 1) * chunkSize} documents");
               //}).ConfigureAwait(false);

               //    await Parallel.ForEachAsync(Enumerable.Range(0, 10000), async (index, token) =>
               //{
               //    var opt = await ctx.GetOptionalAsync(_collection, index.ToString()).ConfigureAwait(false);
               //    if (opt == null)
               //        await ctx.InsertAsync(_collection, index.ToString(), documento).ConfigureAwait(false);
               //    else
               //        await ctx.ReplaceAsync(opt, documento).ConfigureAwait(false);
               //    Console.Write(index);
               //    if (index % 100 == 0)
               //    {
               //                     Console.Clear();
               //                     Console.Write($"Staged {index} documents");
               //    }
               //}).ConfigureAwait(false);
               await ctx.CommitAsync().ConfigureAwait(false);
           }).ConfigureAwait(false);
           watch.Stop();
           var elapsedMs = watch.ElapsedMilliseconds;
           Console.Clear();
           Console.WriteLine(elapsedMs / 1000 + "s");
       }
       catch (TransactionCommitAmbiguousException e)
       {
           Console.WriteLine("Transaction possibly committed");
           Console.WriteLine(e);
       }
       catch (TransactionFailedException e)
       {
           Console.WriteLine("Transaction did not reach commit point");
           Console.WriteLine(e);
       }

       //Transaction
       //var stopwatch = Stopwatch.StartNew();
       //try
       //{
       //    var result = await _transactions.RunAsync(async (ctx) =>
       //    {
       //        for (int i = 0; i < 10_000; i++)
       //        {
       //            await ctx.InsertAsync(_collection, $"testDocument2{i}", documento).ConfigureAwait(false);
       //            Console.Clear();
       //            Console.Write($"Staged {i} documents. Time elapsed: {stopwatch.Elapsed.TotalSeconds}s");
       //        }
       //        Console.WriteLine($"Staging documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");
       //        stopwatch.Restart();
       //        await ctx.CommitAsync().ConfigureAwait(false);
       //    }).ConfigureAwait(false);
       //}
       //catch (Exception e)
       //{
       //    Console.WriteLine(e);
       //}
       //stopwatch.Stop();
       //Console.WriteLine($"Committing documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");


    }

    //public async Task BulkInsertInTxn()
    //{
    //    //Connecting to cluster
    //    var clusterOptions = new ClusterOptions
    //    {
    //        UserName = "Administrator",
    //        Password = "password",
    //        ConnectionString = "couchbase://localhost"
    //    };
    //    var cluster = await Cluster.ConnectAsync(clusterOptions).ConfigureAwait(false);
    //    // Initialize the Couchbase cluster
    // //   var options = new ClusterOptions().WithCredentials("test", "Pwd12345!");
    //    // var options = new ClusterOptions().WithCredentials("Administrator", "password");
    // //   var cluster = await Cluster.ConnectAsync("couchbases://cb.oafqjrqaclzjpn68.cloud.couchbase.com", options).ConfigureAwait(false);
    //    // var cluster = await Cluster.ConnectAsync("couchbase://localhost", options).ConfigureAwait(false);
    //    var bucket = await cluster.BucketAsync("test").ConfigureAwait(false);
    //    var scope = await bucket.ScopeAsync("test").ConfigureAwait(false);
    //    var _collection = await scope.CollectionAsync("test").ConfigureAwait(false);
    //    //--------------------------------------

    //    // Creating a 250KB document
    //    var doc = new { Content = new string('A', 260 * 1024) };
    //    //--------------------------------------

    //    //Transaction
    //    var transactions = Transactions.Create(cluster, TransactionConfigBuilder.Create().ExpirationTime(TimeSpan.FromMinutes(25)));
    //    var stopwatch = Stopwatch.StartNew();
    //    try
    //    {
    //        var result = await transactions.RunAsync(async (ctx) =>
    //        {
    //            for (int i = 0; i < 150_000; i++)
    //            {
    //                await ctx.InsertAsync(_collection, $"testDocument2{i}", doc).ConfigureAwait(false);
    //                Console.Clear();
    //                Console.Write($"Staged {i} documents. Time elapsed: {stopwatch.Elapsed.TotalSeconds}s");
    //            }
    //            Console.WriteLine($"Staging documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");
    //            stopwatch.Restart();
    //            await ctx.CommitAsync().ConfigureAwait(false);
    //        }).ConfigureAwait(false);
    //    }
    //    catch (Exception e)
    //    {
    //        Console.WriteLine(e);
    //    }
    //    stopwatch.Stop();
    //    Console.WriteLine($"Committing documents:{stopwatch.Elapsed.TotalSeconds}s, or {stopwatch.Elapsed.TotalMinutes}min");
    //}
}