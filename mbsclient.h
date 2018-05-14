/*
    A C++ client for a GSI MBS stream server.
    Can also asynchronously open a set of LMD (List Mode) files.

    This software uses the MBS API developed a GSI (Gesellschaft für Schwerionenforschung)
    that is licensed under GNU GPLv2+. See GSI_MBS_API/Go4License.txt for more information.

    Copyright (C) 2014 Maxim Singer

    License: GNU GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)

    Source code: https://github.com/virtmax/MbsClient
*/



#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>
#include <vector>
#include <complex>
#include <fstream>
#include <thread>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <condition_variable>
#include <queue>
#include <chrono>
#include <type_traits>
#include <experimental/filesystem>


extern "C"
{
#include "s_filhe_swap.h"
#include "s_bufhe_swap.h"
#include "s_ves10_1.h"
#include "s_ve10_1_swap.h"
#include "s_evhe_swap.h"

#include "fLmd.h"
#include "f_evt.h"
}


#pragma once


class MbsClient
{    
public:
    MbsClient();
    ~MbsClient();

    /**
     * @brief The CONNECTION_OPTION enum
     */
    enum ConnectionOption {stream=0, file, automatic};

    /**
     * @brief Establish a connection to a MBS stream server or a LMD-file.
     *
     * @param mbsSource The host name or IP of a MBS steam server for a stream connection. The name of a LMD file.
     * @param conOpt Connection option. Can be a stream or file.
     * @param poolForNextFile If true, the function will asynchronously seek for next LMD files that have
     *          name structure 'name_number.lmd'.
     * @return true, if the connection is established.
     *
     * @example MbsClient mbsclient;
     *          mbsclient.connect("192.168.20.37", MbsClient::CONNECTION_OPTION::automatic);
     */
    bool connect(std::string mbsSource, ConnectionOption conOpt, bool poolForNextFile);

    /**
     * @brief Read data from a set of LMD files.
     *
     * @param fileList A list with LMD file names.
     * @param poolForNextFile If true, the function will asynchronously seek for next LMD files that have
     *          name structure 'name_number.lmd'.
     * @return true, if the first file can be opened.
     *
     * @example MbsClient mbsclient;
     *          mbsclient.connect({"data_0023.lmd", "data_0124.lmd"}, true);
     */
    bool connect(std::vector<std::string> fileList, bool poolForNextFile);

    /**
     * @brief Close the connection to the MBS stream server or close the current LMD file.
     *
     * @return true, if the operation was successful.
     */
    bool disconnect();

    /**
     * @brief Return client status.
     * @return true, if a connection is established.
     */
    bool isConnected() const { return !disconnected; }

    /**
     * @brief Give the number of the MBS events stored in the event buffer.
     * @return The number of MBS events stored in the event buffer.
     */
    size_t getEventsInBuffer() const { return eventBuffer.size();}

    /**
     * @brief Clear the MBS event data list.
     */
    void clearEventBuffer();

    /**
     * @brief Return the number of received events.
     * @return The number of received events.
     */
    size_t getNumberOfReceivedEvents() const;

    /**
     * @brief Return the size of the received data in bytes.
     *
     * @return The size of the received data in bytes.
     */
    size_t getSizeOfReceivedData() const;

    /**
     * @brief Return the number of event in the event list.
     *
     * @return The number of events in the event list.
     */
    size_t getNumberOfEventsInBuffer() const;

    /**
     * @brief Return the name of the MBS Server.
     *
     * @return The name of the MBS Server
     */
    std::string getEventServerName() const;


    struct MbsEvent
    {
        long long timestamp;    // unix time in milliseconds (sometimes the same between events...)
        std::vector<uint32_t> data; // raw data
    };

    /**
     * @brief Copy the received MBS data from the eventBuffer to the dest-vector.
     *
     * @param dest The destination vector for the event data.
     * @param NumOfElementsToCopy The number of elements have to be copied from eventBuffer to the dest-vector.
     */
    void getEventData(std::vector<MbsClient::MbsEvent>& dest, size_t nElementsToCopy);

private:

    /**
     * @brief Extract data from MBS stream/file and put it into the eventBuffer. Called by the receiverThread automaticaly.
     */
    void eventReceiver();

    /**
     * @brief Open a single LMD File or a connection to a MBS server.
     * @param mbsSource Filename.
     * @param sourceType GETEVT__FILE/GETEVT__STREAM
     * @return true, if successful
     */
    bool openLmdFile(std::string mbsSource, INTS4 sourceType);

    /**
     * @brief Seek for a new LMD file. Called by fileseekThread.
     */
    void newFileSeeker();

    // buffer for received mbs events
    std::deque<MbsEvent> eventBuffer;

    std::vector<std::string> fileList;
    size_t currentFileIndex = 0;

    // thread stuff for reading the data
    std::mutex queueMutex;
    std::atomic_bool disconnected;
    std::vector<std::thread> receiverThread;

    // thread stuff for seeking for a new file
    std::mutex filelistMutex;
    std::vector<std::thread> fileseekThread;

    // used by the MBS API
    s_evt_channel *inputChannel;
    s_filhe *fileHeader;
    s_bufhe *bufferHeader;

    std::string mbsSource;
    std::atomic<size_t> nEventsInBuffer;
    std::atomic<size_t> nReceivedEvents;
    std::atomic<size_t> sizeOfReceivedData;   // in bytes
};

