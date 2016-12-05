/*   Copyright 2016 Cinegy GmbH

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using CommandLine;
using Newtonsoft.Json;
using TsAnalyser.Logging;
using TsAnalyser.Metrics;
using static System.String;

namespace TsAnalyser
{
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class Program
    {
        private const int WarmUpTime = 500;

        private static bool _receiving;
        private static Options _options;

        private static bool _warmedUp;

        private static DateTime _startTime = DateTime.UtcNow;
        private static bool _pendingExit;
        private static readonly UdpClient UdpClient = new UdpClient { ExclusiveAddressUse = false };

        private static readonly object LogfileWriteLock = new object();
        private static StreamWriter _logFileStream;

        private static readonly RingBuffer RingBuffer = new RingBuffer();

        private static NetworkMetric _networkMetric;
        private static RtpMetric _rtpMetric = new RtpMetric();

        private static readonly StringBuilder ConsoleDisplay = new StringBuilder(1024);
        private static Timer _periodicDataTimer;

        static int Main(string[] args)
        {
            if (args == null || args.Length == 0)
            {
                return RunStreamInteractive();
            }

            if ((args.Length == 1) && (File.Exists(args[0])))
            {
                //a single argument was used, and it was a file - so skip all other parsing
                return Run(new ReadOptions { FileInput = args[0] });
            }

            var result = Parser.Default.ParseArguments<StreamOptions, ReadOptions>(args);

            return result.MapResult(
                (StreamOptions opts) => Run(opts),
                (ReadOptions opts) => Run(opts),
                errs => CheckArgumentErrors());
        }

        private static int CheckArgumentErrors()
        {
            //will print using library the appropriate help - now pause the console for the viewer
            Console.WriteLine("Hit enter to quit");
            Console.ReadLine();
            return -1;
        }

        private static int RunStreamInteractive()
        {
            Console.WriteLine("No arguments supplied - would you like to enter interactive mode? [Y/N]");
            var response = Console.ReadKey();

            if (response.Key != ConsoleKey.Y)
            {
                Console.WriteLine("\n\n");
                Parser.Default.ParseArguments<StreamOptions, ReadOptions>(new string[] { });
                return CheckArgumentErrors();
            }

            var newOpts = new StreamOptions();
            //ask the user interactively for an address and group
            Console.WriteLine(
                "\nYou chose to run in interactive mode, so now you can now set up a basic stream monitor. Making no entry uses defaults.");

            Console.Write("\nPlease enter the multicast address to listen to [239.1.1.1]: ");
            var address = Console.ReadLine();

            if (IsNullOrWhiteSpace(address)) address = "239.1.1.1";

            newOpts.MulticastAddress = address;

            Console.Write("Please enter the multicast group port [1234]: ");
            var port = Console.ReadLine();

            if (IsNullOrWhiteSpace(port))
            {
                port = "1234";
            }

            newOpts.MulticastGroup = int.Parse(port);

            Console.Write("Please enter the adapter address to listen for multicast packets [0.0.0.0]: ");

            var adapter = Console.ReadLine();

            if (IsNullOrWhiteSpace(adapter))
            {
                adapter = "0.0.0.0";
            }

            newOpts.AdapterAddress = adapter;

            return Run(newOpts);
        }

        private static int Run(Options opts)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;

            Console.WriteLine(
                // ReSharper disable once AssignNullToNotNullAttribute
                $"Cinegy Transport Stream Monitoring and Analysis Tool (Built: {File.GetCreationTime(Assembly.GetExecutingAssembly().Location)})\n");

            try
            {
                Console.CursorVisible = false;
                Console.SetWindowSize(120, 60);
            }
            catch
            {
                Console.WriteLine("Failed to increase console size - probably screen resolution is low");
            }
            _options = opts;

            WorkLoop();

            return 0;
        }

        ~Program()
        {
            Console.CursorVisible = true;
        }

        private static void WorkLoop()
        {
            Console.Clear();

            if (!_receiving)
            {
                _receiving = true;

                LogMessage($"Logging started {Assembly.GetExecutingAssembly().GetName().Version}.");

                _periodicDataTimer = new Timer(UpdateSeriesDataTimerCallback, null, 0, 5000);

                SetupMetricsAndDecoders();

                var streamOptions = _options as StreamOptions;
                if (streamOptions != null)
                {

                    StartListeningToNetwork(streamOptions.MulticastAddress, streamOptions.MulticastGroup, streamOptions.AdapterAddress);
                }
            }

            Console.Clear();

            while (!_pendingExit)
            {
                if (!_options.SuppressOutput)
                {
                    PrintConsoleFeedback();
                }

                Thread.Sleep(20);
            }

            LogMessage("Logging stopped.");
        }

        private static void PrintConsoleFeedback()
        {
            var runningTime = DateTime.UtcNow.Subtract(_startTime);

            Console.SetCursorPosition(0, 0);

            if ((_options as StreamOptions) != null)
            {
                PrintToConsole("URL: rtp://@{0}:{1}\tRunning time: {2:hh\\:mm\\:ss}\t\t", ((StreamOptions)_options).MulticastAddress,
                    ((StreamOptions)_options).MulticastGroup, runningTime);

                PrintToConsole(
                    "\nNetwork Details\n----------------\nTotal Packets Rcvd: {0} \tBuffer Usage: {1:0.00}%/(Peak: {2:0.00}%)\t\t\nTotal Data (MB): {3}\t\tPackets per sec:{4}",
                    _networkMetric.TotalPackets, _networkMetric.NetworkBufferUsage, _networkMetric.PeriodMaxNetworkBufferUsage,
                    _networkMetric.TotalData / 1048576,
                    _networkMetric.PacketsPerSecond);
                PrintToConsole("Period Max Packet Jitter (ms): {0}\t\t",
                    _networkMetric.PeriodLongestTimeBetweenPackets);

                PrintToConsole(
                    "Bitrates (Mbps): {0:0.00}/{1:0.00}/{2:0.00}/{3:0.00} (Current/Avg/Peak/Low)\t\t\t",
                    (_networkMetric.CurrentBitrate / 1048576.0), _networkMetric.AverageBitrate / 1048576.0,
                    (_networkMetric.HighestBitrate / 1048576.0), (_networkMetric.LowestBitrate / 1048576.0));

                if (!((StreamOptions)_options).NoRtpHeaders)
                {
                    PrintToConsole(
                        "\nRTP Details\n----------------\nSeq Num: {0}\tMin Lost Pkts: {1}\nTimestamp: {2}\tSSRC: {3}\t",
                        _rtpMetric.LastSequenceNumber, _rtpMetric.EstimatedLostPackets, _rtpMetric.LastTimestamp,
                        _rtpMetric.Ssrc);
                }
            }

            Console.WriteLine(ConsoleDisplay.ToString());
            ConsoleDisplay.Clear();
        }


        private static void StartListeningToNetwork(string multicastAddress, int multicastGroup,
            string listenAdapter = "")
        {

            var listenAddress = IsNullOrEmpty(listenAdapter) ? IPAddress.Any : IPAddress.Parse(listenAdapter);

            var localEp = new IPEndPoint(listenAddress, multicastGroup);

            UdpClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            UdpClient.Client.ReceiveBufferSize = 1500 * 3000;
            UdpClient.ExclusiveAddressUse = false;
            UdpClient.Client.Bind(localEp);
            _networkMetric.UdpClient = UdpClient;

            var parsedMcastAddr = IPAddress.Parse(multicastAddress);
            UdpClient.JoinMulticastGroup(parsedMcastAddr, listenAddress);

            var ts = new ThreadStart(delegate
            {
                ReceivingNetworkWorkerThread(UdpClient, localEp);
            });

            var receiverThread = new Thread(ts) { Priority = ThreadPriority.Highest };

            receiverThread.Start();

            var queueThread = new Thread(ProcessQueueWorkerThread) { Priority = ThreadPriority.AboveNormal };

            queueThread.Start();
        }

        private static void ReceivingNetworkWorkerThread(UdpClient client, IPEndPoint localEp)
        {
            while (_receiving)
            {
                var data = client.Receive(ref localEp);
                if (data == null) continue;

                if (_warmedUp)
                {
                    RingBuffer.Add(ref data);
                }
                else
                {
                    if (DateTime.UtcNow.Subtract(_startTime) > new TimeSpan(0, 0, 0, 0, WarmUpTime))
                        _warmedUp = true;
                }
            }
        }

        private static void ProcessQueueWorkerThread()
        {
            var dataBuffer = new byte[12 + (188 * 7)];

            while (_pendingExit != true)
            {
                int dataSize;
                long timestamp;
                var capacity = RingBuffer.Remove(ref dataBuffer, out dataSize, out timestamp);

                if (capacity > 0)
                {
                    dataBuffer = new byte[capacity];
                    continue;
                }

                if (dataBuffer == null) continue;

                try
                {
                    lock (_networkMetric)
                    {
                        //TODO: Inject ringbuffer delta below (after removing queue)
                        _networkMetric.AddPacket(dataBuffer, timestamp, 0);

                        if (!((StreamOptions)_options).NoRtpHeaders)
                        {
                            _rtpMetric.AddPacket(dataBuffer);
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($@"Unhandled exception within network receiver: {ex.Message}");
                }
            }
            LogMessage("Stopping analysis thread due to exit request.");
        }


        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.CursorVisible = true;
            if (_pendingExit) return; //already trying to exit - allow normal behaviour on subsequent presses
            _pendingExit = true;
            e.Cancel = true;
        }

        private static void RtpMetric_SequenceDiscontinuityDetected(object sender, EventArgs e)
        {
            if (_options.VerboseLogging)
            {
                LogMessage(new LogRecord()
                {
                    EventCategory = "Warn",
                    EventKey = "RtpSkip",
                    EventTags = _options.DescriptorTags,
                    EventMessage = "Discontinuity in RTP sequence."
                });
            }
        }

        private static void NetworkMetric_BufferOverflow(object sender, EventArgs e)
        {
            LogMessage(new LogRecord()
            {
                EventCategory = "Error",
                EventKey = "NetworkBuffer",
                EventTags = _options.DescriptorTags,
                EventMessage = "Network buffer > 99% - probably loss of data from overflow."
            });
        }

        private static void PrintToConsole(string message, params object[] arguments)
        {
            if (_options.SuppressOutput) return;

            ConsoleDisplay.AppendLine(Format(message, arguments));
        }

        private static void LogMessage(string message)
        {
            var logRecord = new LogRecord()
            {
                EventCategory = "Info",
                EventKey = "GenericEvent",
                EventTags = _options.DescriptorTags,
                EventMessage = message
            };
            LogMessage(logRecord);
        }

        private static void LogMessage(LogRecord logRecord)
        {
            var formattedMsg = JsonConvert.SerializeObject(logRecord);
            ThreadPool.QueueUserWorkItem(WriteToFile, formattedMsg);
        }

        private static void WriteToFile(object line)
        {
            lock (LogfileWriteLock)
            {
                try
                {
                    if (_logFileStream == null || _logFileStream.BaseStream.CanWrite != true)
                    {
                        if (IsNullOrWhiteSpace(_options.LogFile)) return;

                        var fs = new FileStream(_options.LogFile, FileMode.Append, FileAccess.Write);

                        _logFileStream = new StreamWriter(fs) { AutoFlush = true };
                    }
                    _logFileStream.WriteLine(line);
                }
                catch (Exception)
                {
                    Debug.WriteLine("Concurrency error writing to log file...");
                    _logFileStream?.Close();
                    _logFileStream?.Dispose();
                }
            }
        }

        private static void UpdateSeriesDataTimerCallback(object o)
        {
            if (!_options.TimeSeriesLogging) return;

            try
            {
                var tsMetricLogRecord = new TsMetricLogRecord()
                {
                    EventCategory = "Info",
                    EventKey = "Metric",
                    EventTags = _options.DescriptorTags,
                    Net = _networkMetric
                };

                if (!((StreamOptions)_options).NoRtpHeaders)
                {
                    tsMetricLogRecord.Rtp = _rtpMetric;
                }

                var formattedMsg = JsonConvert.SerializeObject(tsMetricLogRecord);

                WriteToFile(formattedMsg);
            }
            catch (Exception)
            {
                Debug.WriteLine("Concurrency error writing to log file...");
                _logFileStream?.Close();
                _logFileStream?.Dispose();
            }

        }

        private static void SetupMetricsAndDecoders()
        {

            _startTime = DateTime.UtcNow;

            var streamOpts = _options as StreamOptions;

            if (streamOpts != null)
            {
                _networkMetric = new NetworkMetric()
                {
                    MaxIat = streamOpts.InterArrivalTimeMax,
                    MulticastAddress = streamOpts.MulticastAddress,
                    MulticastGroup = streamOpts.MulticastGroup
                };

                _rtpMetric = new RtpMetric();

                _rtpMetric.SequenceDiscontinuityDetected += RtpMetric_SequenceDiscontinuityDetected;
                _networkMetric.BufferOverflow += NetworkMetric_BufferOverflow;
                _networkMetric.UdpClient = UdpClient;
            }

        }
    }
}

