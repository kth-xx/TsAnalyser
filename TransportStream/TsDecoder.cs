﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using TsAnalyser.Metrics;
using TsAnalyser.Tables;

namespace TsAnalyser.TransportStream
{
    public class TsDecoder
    {

        public ProgramAssociationTable ProgramAssociationTable => _patFactory.ProgramAssociationTable;
        public ServiceDescriptionTable ServiceDescriptionTable => _sdtFactory.ServiceDescriptionTable;

        public List<ProgramMapTable> ProgramMapTables { get; private set; }
        
        private ProgramAssociationTableFactory _patFactory;
        private ServiceDescriptionTableFactory _sdtFactory;
        private List<ProgramMapTableFactory> _pmtFactories;

        public TsDecoder()
        {
            SetupFactories();
        }
        
        public void AddPacket(TsPacket newPacket)
        {
            try
            {
                if (newPacket.TransportErrorIndicator) return;

                switch (newPacket.Pid)
                {
                    case (short)PidType.PatPid:
                        _patFactory.AddPacket(newPacket);
                        break;
                    case (short)PidType.SdtPid:
                        _sdtFactory.AddPacket(newPacket);
                        break;
                    default:
                        CheckPmt(newPacket);
                        break;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Exception generated within AddPacket method: " + ex.Message);
            }
        }

        public ServiceDescriptor GetServiceDescriptorForProgramNumber(int? programNumber)
        {
            if (programNumber == null) return null;

            var serviceDescItem = _sdtFactory?.ServiceDescriptionItems?.SingleOrDefault(
                                  i => i.ServiceId == programNumber);

            var serviceDesc =
                serviceDescItem?.Descriptors?.SingleOrDefault(sd => (sd as ServiceDescriptor) != null) as ServiceDescriptor;

            return serviceDesc;
        }

        public T GetDescriptorForProgramNumberByTag<T>( int? programNumber, int streamType, int descriptorTag)  where T : class
        {
            if (programNumber == null) return null;
            
            var selectedPmt = ProgramMapTables?.FirstOrDefault(t => t.ProgramNumber == programNumber);

            if (selectedPmt == null) return null;

            var selectedDesc = default(T);

            foreach (var esStream in selectedPmt.EsStreams)
            {
                if (esStream.StreamType != streamType) continue;

                selectedDesc = esStream.Descriptors.SingleOrDefault(d => d.DescriptorTag == descriptorTag) as T;

                if (selectedDesc != null) break;
            }
        
            return selectedDesc;
            
        }

        public EsInfo GetEsStreamForProgramNumberByTag(int? programNumber, int streamType, int descriptorTag) 
        {
            if (programNumber == null) return null;

            var selectedPmt = ProgramMapTables?.FirstOrDefault(t => t.ProgramNumber == programNumber);

            if (selectedPmt == null) return null;

            foreach (var esStream in selectedPmt.EsStreams)
            {
                if (esStream.StreamType != streamType) continue;

                var desc = esStream.Descriptors.SingleOrDefault(d => d.DescriptorTag == descriptorTag);

                if (desc != null) return esStream;
               
            }

            return null;
        }

        private void CheckPmt(TsPacket tsPacket)
        {
            if (ProgramAssociationTable == null) return;

            if (tsPacket.Pid == 0x0010)
            {
                //Pid 0x0010 is a NIT packet
                //TODO: Decode NIT, and store
                return;
            }

            if (!ProgramAssociationTable.Pids.Contains(tsPacket.Pid)) return;

            var selectedPmt = _pmtFactories?.FirstOrDefault(t => t.TablePid == tsPacket.Pid);
            if (selectedPmt == null)
            {
                selectedPmt = new ProgramMapTableFactory();
                selectedPmt.TableChangeDetected += _pmtFactory_TableChangeDetected;
                _pmtFactories?.Add(selectedPmt);
            }
            selectedPmt.AddPacket(tsPacket);
        }
        
        private void SetupFactories()
        {
            _patFactory = new ProgramAssociationTableFactory();
            _patFactory.TableChangeDetected += _patFactory_TableChangeDetected;
            _pmtFactories = new List<ProgramMapTableFactory>(16);
            ProgramMapTables = new List<ProgramMapTable>(16);

            _sdtFactory = new ServiceDescriptionTableFactory();
            _sdtFactory.TableChangeDetected += _sdtFactory_TableChangeDetected;
        }

        public ProgramMapTable GetSelectedPmt(int programNumber)
        {
            ProgramMapTable pmt;

            if (programNumber == 0)
            {
                if (ProgramMapTables?.Count == 0) return null;
                if (ProgramAssociationTable == null) return null;
                //without a passed program number, use the default program
                if (ProgramMapTables?.Count <
                    (ProgramAssociationTable?.Pids?.Length - 1)) return null;

                pmt = ProgramMapTables?.OrderBy(t => t.ProgramNumber).FirstOrDefault();
            }
            else
            {
                pmt = ProgramMapTables?.SingleOrDefault(t => t.ProgramNumber == programNumber);
            }

            return pmt;
        }

        private void _sdtFactory_TableChangeDetected(object sender, TransportStreamEventArgs args)
        {
            Debug.WriteLine($"SDT {args.TsPid}, Version: {ServiceDescriptionTable.VersionNumber} - Section Num: {ServiceDescriptionTable.SectionNumber} refreshed");
            OnTableChangeDetected();
        }

        private void _pmtFactory_TableChangeDetected(object sender, TransportStreamEventArgs e)
        {
            lock (this)
            {
                var fact = sender as ProgramMapTableFactory;

                if (fact == null) return;

                var selectedPmt = ProgramMapTables?.FirstOrDefault(t => t.Pid == e.TsPid);

                if (selectedPmt != null)
                {
                    ProgramMapTables?.Remove(selectedPmt);
                    Debug.WriteLine($"PMT {e.TsPid} refreshed");
                }
                else
                {
                    Debug.WriteLine($"PMT {e.TsPid} added");
                }

                ProgramMapTables?.Add(fact.ProgramMapTable);
            }

            OnTableChangeDetected();
        }

        private void _patFactory_TableChangeDetected(object sender, TransportStreamEventArgs e)
        {
            Debug.WriteLine("PAT refreshed");
            OnTableChangeDetected();
        }

        //A decoded table change has been processed
        public event EventHandler TableChangeDetected;

        private void OnTableChangeDetected()
        {
            var handler = TableChangeDetected;
            handler?.Invoke(this, EventArgs.Empty);
        }
    }


}

