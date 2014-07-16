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
using Newtonsoft.Json;
using Legacy.Common.Models;
using System.Text;
using System.IO;
using System.Net.Mail;
using Legacy.Common;

namespace Legacy.EmailWorker
{
  public class EmailWorkerRole : RoleEntryPoint
  {
    public override void Run()
    {
      //------------------------------------------------------------------------------
      // 1. Establish connection to Storage
      //------------------------------------------------------------------------------
      var connString = CloudConfigurationManager.GetSetting("StorageConnectionString");
      var storageAccount = CloudStorageAccount.Parse(connString);

      var queueClient = storageAccount.CreateCloudQueueClient();
      var incoming = queueClient.GetQueueReference("report-queue");
      incoming.CreateIfNotExists();

      var blobClient = storageAccount.CreateCloudBlobClient();
      var container = blobClient.GetContainerReference("reports");
      container.CreateIfNotExists();

      //------------------------------------------------------------------------------
      // 2. Loop and look for new messages
      //------------------------------------------------------------------------------
      while (true)
      {
        var msg = incoming.GetMessage();
        if (null != msg)
        {
          var content = msg.AsString;
          var searchRequest = JsonConvert.DeserializeObject<ReportRequest>(content);

          var blob = container.GetBlockBlobReference(searchRequest.blobname);
          MemoryStream report = new MemoryStream();
          blob.DownloadToStream(report);
          report.Position = 0;

          //------------------------------------------------------------------------------
          // 3. Send the report
          //------------------------------------------------------------------------------
          EmailHelper.SendReport(report, searchRequest.email, searchRequest.term);

          //------------------------------------------------------------------------------
          // 4. Delete the meesage from the queue
          //------------------------------------------------------------------------------
          incoming.DeleteMessage(msg);
        }
        Thread.Sleep(10000);
      }
    }
  }
}
