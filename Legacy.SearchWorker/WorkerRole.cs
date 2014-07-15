using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Tweetinvi;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Legacy.Common.Models;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Legacy.SearchWorker
{
  public class WorkerRole : RoleEntryPoint
  {
    public override void Run()
    {
      var connString = CloudConfigurationManager.GetSetting("StorageConnectionString");
      var storageAccount = CloudStorageAccount.Parse(connString);

      var queueClient = storageAccount.CreateCloudQueueClient();
      var incoming = queueClient.GetQueueReference("search-queue");
      incoming.CreateIfNotExists();

      var output = queueClient.GetQueueReference("report-queue");
      output.CreateIfNotExists();

      var blobClient = storageAccount.CreateCloudBlobClient();
      var container = blobClient.GetContainerReference("reports");
      container.CreateIfNotExists();

      TwitterCredentials.SetCredentials("6849932-2N0S9toLkYRJBP1YwBbgZpOwxt6pdWPvwmz9h8OZnz", "jvNCi9jf9e6yX6PRBHIoap7NIqK7EHxJQcRLQQfjmc0hT", "lR5vtVlrK3fskKSM7hRLIKiRa", "yd7f7LJl8nrDlijDquqd2uTdmnNsT6BHIGjr5JymwGiU367Kpw");

      while (true)
      {
        var msg = incoming.GetMessage();
        if (null != msg)
        {
          var content = msg.AsString;
          var searchRequest = JsonConvert.DeserializeObject<SearchRequest>(content);

          // Search Twitter and create the report
          var tweets = Search.SearchTweets(searchRequest.term);
          var query = from t in tweets
                      select (new string[]
                      {
                        t.IdStr,
                        t.TweetLocalCreationDate.ToString(),
                        t.Creator.ScreenName,
                        t.Text
                      }).Aggregate((a, b)=>a + ',' + b);
          var report = query.Aggregate((a, b) => a + "\r\n" + b);

          // Upload the report to Blob Storage
          string blobName = string.Format("{0}/{1}.csv", DateTime.Now.ToString("yyyyMMddHHmmssffff"), searchRequest.term);
          var blob = container.GetBlockBlobReference(blobName);
          blob.UploadText(report);

          // Create a queue message with a link to the report
          ReportRequest reportRequest = new ReportRequest {
            email = searchRequest.email,
            term = searchRequest.term,
            report_url = blob.StorageUri.PrimaryUri.AbsoluteUri
          };
          var serialized = JsonConvert.SerializeObject(reportRequest);
          var message = new CloudQueueMessage(serialized);
          output.AddMessage(message);

          incoming.DeleteMessage(msg);
        }
        Thread.Sleep(10000);
      }
    }

  }
}
