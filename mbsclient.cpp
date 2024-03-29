/*
    A C++ client for a GSI MBS stream server.
    Can also asynchronously open a set of LMD (List Mode) files.

    This software uses the MBS API developed a GSI (Gesellschaft für Schwerionenforschung)
    that is licensed under GNU GPLv2+. See GSI_MBS_API/Go4License.txt for more information.

    Copyright (C) 2014-2023 Maxim Singer

    License: GNU GPLv3 (https://www.gnu.org/licenses/gpl-3.0.html)

    Source code: https://github.com/virtmax/MbsClient
*/



#include "mbsclient.h"

namespace fs = std::filesystem;

MbsClient::MbsClient() : mbsSource("not connected")
{
    disconnected = true;
    sizeOfReceivedData = 0;
    nEventsInBuffer = 0;
    nReceivedEvents = 0;
    maxEventBufferSize = 1e6;

    inputChannel = nullptr;
    fileHeader = nullptr;
    bufferHeader = nullptr;
}

MbsClient::~MbsClient()
{
    if(isConnected())
        disconnect();
}

bool MbsClient::connect(std::string mbsSource, ConnectionOption conOpt, bool poolForNextFile)
{
    sizeOfReceivedData = 0;
    nEventsInBuffer = 0;
    nReceivedEvents = 0;
    noMoreEvents = false;

    INTS4 sourceType = 0;
    if(conOpt == ConnectionOption::file)
        sourceType = GETEVT__FILE;
    else if(conOpt== ConnectionOption::stream)
        sourceType = GETEVT__STREAM;
    else if(conOpt== ConnectionOption::automatic)
    {
        if(mbsSource.size() < 5)
        {
            std::cout << "MbsClient::connect : The source name is too short (length < 5). " << std::endl;
            return false;
        }

        std::string file_ext =  mbsSource.substr(mbsSource.size()-3, mbsSource.size()-1);
        std::transform(file_ext.begin(), file_ext.end(), file_ext.begin(), ::tolower);

        if(file_ext == "lmd")
            sourceType = GETEVT__FILE;
        else
            sourceType = GETEVT__STREAM;
    }
    else
    {
        std::cout << "MbsClient::connect: CONNECTION_OPTION must be file or stream." << std::endl;
    }

    if(sourceType != GETEVT__FILE)
    {
        std::cout << "MbsClient::connect: option for seeking for a next file is not possible"
                  << "for stream connections. ignore." << std::endl;
        poolForNextFile = false;
    }

    filelist.push_back(mbsSource);


    if(openLmdFile(filelist.at(0), sourceType))
    {
        if(poolForNextFile)
            fileseekThread.push_back(std::thread(&MbsClient::newFileSeeker, this));

        receiverThread.push_back(std::thread(&MbsClient::eventReceiver, this));
        return true;
    }
    else
        return false;
}

bool MbsClient::connect(std::vector<std::string> fileList, bool poolForNextFile)
{
    if(fileList.size() == 0)
        return false;

    this->filelist = fileList;

    sizeOfReceivedData = 0;
    nEventsInBuffer = 0;
    nReceivedEvents = 0;
    noMoreEvents = false;

    if(openLmdFile(fileList.at(0), GETEVT__FILE))
    {
        if(poolForNextFile)
            fileseekThread.push_back(std::thread(&MbsClient::newFileSeeker, this));

        receiverThread.push_back(std::thread(&MbsClient::eventReceiver, this));
        return true;
    }
    else
        return false;
}


bool MbsClient::openLmdFile(std::string mbsSource, INTS4 sourceType)
{
    inputChannel = nullptr;
    fileHeader = nullptr;
    bufferHeader = nullptr;

    // initialize the input channel
    inputChannel = f_evt_control();

    /*+   first argument of f_evt_get_open()    : Type of server:         */
    /*-               GETEVT__FILE   : Input from file                    */
    /*-               GETEVT__STREAM : Input from MBS stream server       */
    /*-               GETEVT__TRANS  : Input from MBS transport           */
    /*-               GETEVT__EVENT  : Input from MBS event server        */
    /*-               GETEVT__REVSERV: Input from remote event server     */
    //   second argument of f_evt_get_open()    : name of server
    int32_t result = f_evt_get_open(sourceType, mbsSource.c_str(), inputChannel,
                                    (CHARS**) (&fileHeader), 1, 0);

    if(result != GETEVT__SUCCESS)
    {
        std::cout << "MbsClient::connect: Can't open '" << mbsSource
                  << "': result != GETEVT__SUCCESS. Is the file path or the IP address correct?" << std::endl;
        return false;
    }
    else
    {
        std::cout << "MbsClient::connect: Connection successful." << std::endl;
    }

    this->mbsSource = mbsSource;

    if (fileHeader != nullptr)
    {
        std::cout << "The event source is open..." << std::endl
                  << "filhe_dlen : " << fileHeader->filhe_dlen << std::endl
                  << "filhe_file : " << fileHeader->filhe_file << std::endl
                  << "filhe_user : " << fileHeader->filhe_user << std::endl;
    }

    disconnected = false;
    return true;
}

void MbsClient::newFileSeeker()
{
    while(!disconnected)
    {
        auto fullpath = fs::path(filelist.back());
        auto filename = fullpath.filename();
        auto dirPath = fullpath.parent_path();

        // extract the file number from the file name. Format filename_number.lmd
        auto underline_pos = filename.string().rfind('_');
        if(underline_pos == std::string::npos)
        {
            std::cout << "MbsClient::connect: no '_' in filename found. "
                      << "Can't extract the file number. Format: filename_number.lmd" << std::endl;
            return;
        }
        std::string numberPart =  filename.string().substr(underline_pos+1, filename.string().size()-underline_pos-5);
        uint32_t number = 0;

        try
        {
            number = std::stoul(numberPart, nullptr);
        }
        catch(...)
        {
            std::cout << "MbsClient::connect: Can't extract the file number." << std::endl;
            return;
        }

        std::stringstream ss;
        ss << std::setw(numberPart.size()) << std::setfill('0') << (number+1);

        std::string nextFilePath = dirPath.string() + "/"
                + filename.string().substr(0, underline_pos) + "_" + ss.str() + ".lmd";

        if(fs::exists(nextFilePath))
        {
            std::cout << "Next LMD file '"<< nextFilePath
                      <<"' will be opened automatically after the previous file is have been analyzed."<< std::endl;

            // acquire lock
            std::unique_lock<std::mutex> ulock(filelistMutex);

            filelist.push_back(nextFilePath);
        }
		else
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}



bool MbsClient::disconnect()
{
    // acquire lock
    std::unique_lock<std::mutex> lock(queueMutex);

    disconnected = true;
    lock.unlock();

    for(size_t i = 0; i < receiverThread.size();i++)
    {
        receiverThread.at(i).join();
    }
    receiverThread.clear();

    for(size_t i = 0; i < fileseekThread.size();i++)
    {
        fileseekThread.at(i).join();
    }
    fileseekThread.clear();

    if(inputChannel != nullptr)
        f_evt_get_close(inputChannel);
    inputChannel = nullptr;

    fileHeader = nullptr;
    bufferHeader = nullptr;
    mbsSource = "not connected";
    return true;
}

void MbsClient::setBufferLimit(size_t maxEventBufferSize)
{
    this->maxEventBufferSize = maxEventBufferSize;
}

void MbsClient::eventReceiver()
{
    int32_t *eventData = nullptr;
    int mess = 0;
    while(inputChannel != nullptr && disconnected==false)
    {
        int32_t result = 0;
        eventData = nullptr;
        result = f_evt_get_event(inputChannel, &eventData, (INTS4**) (&bufferHeader));

        if(result == GETEVT__NOMORE)
        {
            std::cout << "size_of_received_data=" << sizeOfReceivedData << std::endl
                      << "Close "<<mbsSource << std::endl;
            f_evt_get_close(inputChannel);

            if(filelist.size() > currentFileIndex+1)
            {
                currentFileIndex++;
                std::string next_mbs_source = filelist.at(currentFileIndex);

                std::cout << "Try to open " << next_mbs_source<< std::endl;

                if(!openLmdFile(next_mbs_source, GETEVT__FILE))
                {
                    std::cout << "error: if(!openLmdFile(next_mbs_source, GETEVT__FILE)). next_mbs_source="
                              << next_mbs_source << std::endl;
                    return;
                }
            }
            else
            {
                noMoreEvents = true;
            }
        }


        if(result == GETEVT__FRAGMENT && mess < 10)
        {
            std::cout << "event fragment found..." << std::endl;
            std::cout << "f_evt_type(...) output: " << std::endl;
            f_evt_type(bufferHeader, (s_evhe*) eventData, -1, 0, 1, 0);
            std::cout << "----------------------------------------------------" << std::endl;
            mess++;
        }

        if(result != GETEVT__SUCCESS)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // wait to reduce CPU load
            continue;
        }

        noMoreEvents = false;
        if(nEventsInBuffer > maxEventBufferSize)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); // wait to reduce CPU load
        }

        // uncomment the following lines to output the "raw data and header info from the event"
        /*
        if(mess > 0)
        {
            std::cout << "f_evt_type(...) output: " << std::endl;
            f_evt_type(bufferHeader, (s_evhe*) eventData, -1, 0, 1, 0);
            std::cout << "----------------------------------------------------" << std::endl;
        }*/
        uint64_t mbsTimestamp = static_cast<uint64_t>(bufferHeader->l_time[0])*1000
                                    + static_cast<uint64_t>(bufferHeader->l_time[1]);
        // if(this->eventBuffer.size()==0)
        //    std::cout << bufferHeader->l_time[0] << " "<< bufferHeader->l_time[1] << "  " << mbsTimestamp << std::endl;

        MbsEvent mbsevent;
        mbsevent.timestamp = mbsTimestamp;

        // acquire lock
        std::unique_lock<std::mutex> ulock(queueMutex); 
        for(int sub = 1; result != GETEVT__NOMORE; ++sub)
        {
            s_ves10_1 *subeventHeader = nullptr;
            int32_t *data = nullptr;
            int32_t dataLength = 0;

            result = f_evt_get_subevent((s_ve10_1*) eventData, sub,
                                        (int32_t**) &subeventHeader, &data, &dataLength);
            if(result == GETEVT__SUCCESS)
            {
                if(dataLength > 0)
                {
                    mbsevent.data.assign(data, data+dataLength);
                    eventBuffer.push_back(mbsevent);

                    sizeOfReceivedData += dataLength*sizeof(int32_t);
                    nReceivedEvents++;
                }
            }
        }

        nEventsInBuffer = eventBuffer.size();
    }

    if(disconnected)
        return;
}

void MbsClient::clearEventBuffer()
{
    // acquire the mutex lock
    std::unique_lock<std::mutex> ulock(queueMutex);

    if(ulock.owns_lock())
    {
        eventBuffer.clear();
        nEventsInBuffer = 0;
    }
}

void MbsClient::getEventData(std::vector<MbsClient::MbsEvent> &dest, size_t nElementsToCopy)
{
    // acquire the mutex lock
    std::unique_lock<std::mutex> ulock(queueMutex, std::try_to_lock);

    if(ulock.owns_lock())
    {
        nElementsToCopy = std::min<size_t>(nElementsToCopy, eventBuffer.size());
        if(nElementsToCopy > 0)
        {
            dest.insert(dest.end(), std::make_move_iterator(eventBuffer.begin()), std::make_move_iterator(eventBuffer.begin()+nElementsToCopy));
            eventBuffer.erase(eventBuffer.begin(), eventBuffer.begin()+nElementsToCopy);          
        }

        nEventsInBuffer = eventBuffer.size();
    }
}

size_t MbsClient::getSizeOfReceivedData() const
{
    return sizeOfReceivedData;
}

size_t MbsClient::getNumberOfReceivedEvents() const
{
    return nReceivedEvents;
}

size_t MbsClient::getNumberOfEventsInBuffer() const
{
    return eventBuffer.size();
}

std::string MbsClient::getEventServerName() const
{
    return mbsSource;
}

std::vector<std::string> MbsClient::getFilelist() const
{
    return filelist;
}
