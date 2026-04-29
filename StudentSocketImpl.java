import java.net.*;
import java.io.*;
import java.util.Timer;

class StudentSocketImpl extends BaseSocketImpl {

  // SocketImpl data members:
  //   protected InetAddress address;
  //   protected int port;
  //   protected int localport;

  private enum State {
  CLOSED, LISTEN, SYN_SENT, SYN_RCVD, ESTABLISHED,
  FIN_WAIT_1, FIN_WAIT_2, CLOSE_WAIT, CLOSING, LAST_ACK, TIME_WAIT
}

private State state = State.CLOSED;

private InetAddress remoteAddress;
private int remotePort;

// simple seq/ack (for now)
private int seqNum = 100;
private int ackNum = 0;

  private Demultiplexer D;
  private Timer tcpTimer;


  StudentSocketImpl(Demultiplexer D) {  // default constructor
    this.D = D;
  }

  private void changeState(State newState) {
    System.out.println("!!! " + state + " -> " + newState);
    state = newState;
  }

  /**
   * Connects this socket to the specified port number on the specified host.
   *
   * @param      address   the IP address of the remote host.
   * @param      port      the port number.
   * @exception  IOException  if an I/O error occurs when attempting a
   *               connection.
   */
 public synchronized void connect(InetAddress address, int port) throws IOException {
  this.address = address;
  this.port = port;
  this.remoteAddress = address;
  this.remotePort = port;

  localport = D.getNextAvailablePort();

  changeState(State.SYN_SENT);

  System.out.println("localport is " + localport);

  // register connection with demultiplexer
  D.registerConnection(remoteAddress, localport, remotePort, this);

  // send SYN
  TCPPacket synPacket = new TCPPacket(
    localport,
    remotePort,
    seqNum,
    ackNum,
    false,  // ackFlag
    true,   // synFlag
    false,  // finFlag
    0,
    null
  );

  
  TCPWrapper.send(synPacket, remoteAddress);
  createTimerTask(2000, synPacket);  // retry after 2 seconds
  
  System.out.println("About to wait, state=" + state);
  // Wait for connection to be established
  while (state != State.ESTABLISHED) {
    try {
      wait();
    } catch (InterruptedException e) {
      throw new IOException("Connection interrupted");
    }
  }
  System.out.println("Done waiting, state=" + state);
}
  
  /**
   * Called by Demultiplexer when a packet comes in for this connection
   * @param p The packet that arrived
   */
  public synchronized void receivePacket(TCPPacket p) {

  System.out.println("STATE=" + state + " from=" + p.sourceAddr + ":" + p.sourcePort);

  System.out.println("Received packet:");
  System.out.println("SYN=" + p.synFlag + 
                     " ACK=" + p.ackFlag + 
                     " FIN=" + p.finFlag);
  
  // Handle different states
  switch(state) {
    case ESTABLISHED:
    if (p.finFlag) {
        // passive close - remote side closing first
        ackNum = p.seqNum + 1;
        
        // send ACK
        TCPPacket ackPacket = new TCPPacket(
            localport, remotePort,
            seqNum, ackNum,
            true, false, false, 0, null
        );
        TCPWrapper.send(ackPacket, remoteAddress);
        changeState(State.CLOSE_WAIT);
        
        // immediately send FIN
        TCPPacket finPacket = new TCPPacket(
            localport, remotePort,
            seqNum, ackNum,
            false, false, true, 0, null
        );
        TCPWrapper.send(finPacket, remoteAddress);
        changeState(State.LAST_ACK);
    }
    break;

case LAST_ACK:
    if (p.ackFlag && !p.finFlag) {
        changeState(State.CLOSED);
        notifyAll();
    }
    break;
    case FIN_WAIT_1:
    if (p.ackFlag && !p.synFlag && !p.finFlag) {
        // received ACK
        if (tcpTimer != null) {
    tcpTimer.cancel();
    tcpTimer = null;
}
        seqNum = p.ackNum;
        changeState(State.FIN_WAIT_2);
    } else if (p.finFlag) {
        // simultaneous close - received FIN
        ackNum = p.seqNum + 1;
        TCPPacket ackPacket = new TCPPacket(
            localport, remotePort,
            seqNum, ackNum,
            true, false, false, 0, null
        );
        TCPWrapper.send(ackPacket, remoteAddress);
        changeState(State.CLOSING);
    }
    break;

case FIN_WAIT_2:
    if (p.finFlag) {
        // received FIN
        ackNum = p.seqNum + 1;
        TCPPacket ackPacket = new TCPPacket(
            localport, remotePort,
            seqNum, ackNum,
            true, false, false, 0, null
        );
        TCPWrapper.send(ackPacket, remoteAddress);
        changeState(State.TIME_WAIT);
        createTimerTask(30000, "TIME_WAIT");
    }
    break;

case TIME_WAIT:
    if (p.finFlag) {
        // retransmitted FIN - just resend ACK, don't change state
        TCPPacket ackPacket = new TCPPacket(
            localport, remotePort,
            seqNum, ackNum,
            true, false, false, 0, null
        );
        TCPWrapper.send(ackPacket, remoteAddress);
    }
    break;

case CLOSING:
    if (p.ackFlag && !p.finFlag) {
        // received ACK
        if (tcpTimer != null) {
            tcpTimer.cancel();
            tcpTimer = null;
        }
        changeState(State.TIME_WAIT);
        createTimerTask(30000, "TIME_WAIT");
    }
    break;
    case LISTEN:
      if (p.synFlag && !p.ackFlag) {
        // Received SYN, send SYN-ACK
        System.out.println("LISTEN: Received SYN, sending SYN-ACK");
        
        // Store remote connection info
        remoteAddress = p.sourceAddr;
        remotePort = p.sourcePort;
        ackNum = p.seqNum + 1;
        
        // Unregister as listening socket and register as connection
        try {
          D.unregisterListeningSocket(localport, this);
          D.registerConnection(remoteAddress, localport, remotePort, this);
          System.out.println("Registered connection: " + remoteAddress + ":" + remotePort);
        } catch (IOException e) {
          System.err.println("Failed to register connection: " + e);
        }
        
        // Send SYN-ACK
        TCPPacket synAckPacket = new TCPPacket(
          localport,
          remotePort,
          seqNum,
          ackNum,
          true,   // ackFlag
          true,   // synFlag
          false,  // finFlag
          0,
          null
        );
        changeState(State.SYN_RCVD);
        TCPWrapper.send(synAckPacket, remoteAddress);
        createTimerTask(2000, synAckPacket);
      }
      break;
      
    case SYN_SENT:
      if (p.synFlag && p.ackFlag) {
        // Received SYN-ACK, send ACK to complete handshake
        if (tcpTimer != null) {
            tcpTimer.cancel();
            tcpTimer = null;
        }
        System.out.println("SYN_SENT: Received SYN-ACK, sending ACK");
        
        if (p.ackNum == seqNum + 1) {
          ackNum = p.seqNum + 1;
          seqNum = p.ackNum;
          
          // Send ACK
          TCPPacket ackPacket = new TCPPacket(
            localport,
            remotePort,
            seqNum,
            ackNum,
            true,   // ackFlag
            false,  // synFlag
            false,  // finFlag
            0,
            null
          );
          changeState(State.ESTABLISHED);
          TCPWrapper.send(ackPacket, remoteAddress);
        
        }
      }
      break;
      
    case SYN_RCVD:
      if (p.ackFlag && !p.synFlag) {
        // Received ACK, connection established
        if (tcpTimer != null) {
            tcpTimer.cancel();
            tcpTimer = null;
        }
        System.out.println("SYN_RCVD: Received ACK, connection established");
        
        if (p.ackNum == seqNum + 1) {
          seqNum = p.ackNum;
          changeState(State.ESTABLISHED);
        }
      }
      break;
      
    default:
      // Handle other states (for now, just print)
      break;
  }
  
  // Notify any waiting threads
  notifyAll();
 }
  
  /** 
   * Waits for an incoming connection to arrive to connect this socket to
   * Ultimately this is called by the application calling 
   * ServerSocket.accept(), but this method belongs to the Socket object 
   * that will be returned, not the listening ServerSocket.
   * Note that localport is already set prior to this being called.
   */
  public synchronized void acceptConnection() throws IOException {
  changeState(State.LISTEN);

  this.port = localport;

  System.out.println("Registering listening socket on port " + localport);

  D.registerListeningSocket(localport, this);

  // Wait for connection to be established (SYN_RCVD or ESTABLISHED)
  System.out.println("About to wait, state=" + state);
  while (state != State.ESTABLISHED) {
    try {
      wait();
    } catch (InterruptedException e) {
      throw new IOException("Accept interrupted");
    }
  }
  System.out.println("Done waiting, state=" + state);
 }

  
  /**
   * Returns an input stream for this socket.  Note that this method cannot
   * create a NEW InputStream, but must return a reference to an 
   * existing InputStream (that you create elsewhere) because it may be
   * called more than once.
   *
   * @return     a stream for reading from this socket.
   * @exception  IOException  if an I/O error occurs when creating the
   *               input stream.
   */
  public InputStream getInputStream() throws IOException {
    // project 4 return appIS;
    return null;
    
  }

  /**
   * Returns an output stream for this socket.  Note that this method cannot
   * create a NEW InputStream, but must return a reference to an 
   * existing InputStream (that you create elsewhere) because it may be
   * called more than once.
   *
   * @return     an output stream for writing to this socket.
   * @exception  IOException  if an I/O error occurs when creating the
   *               output stream.
   */
  public OutputStream getOutputStream() throws IOException {
    // project 4 return appOS;
    return null;
  }


  /**
   * Closes this socket. 
   *
   * @exception  IOException  if an I/O error occurs when closing this socket.
   */
  public synchronized void close() throws IOException {

    if (state == State.CLOSED) return;  // already closed
    if (state == State.LAST_ACK) return;  // passive close in progress
    if (state == State.CLOSE_WAIT) return;  // passive close in progress
    

    changeState(State.FIN_WAIT_1);
    
    TCPPacket finPacket = new TCPPacket(
        localport, remotePort,
        seqNum, ackNum,
        false,  // ackFlag
        false,  // synFlag
        true,   // finFlag
        0, null
    );
    TCPWrapper.send(finPacket, remoteAddress);
    createTimerTask(2000, finPacket);
    
    while (state != State.CLOSED) {
        try {
            wait();
        } catch (InterruptedException e) {
            throw new IOException("Close interrupted");
        }
    }
}

  

  /** 
   * create TCPTimerTask instance, handling tcpTimer creation
   * @param delay time in milliseconds before call
   * @param ref generic reference to be returned to handleTimer
   */
  private TCPTimerTask createTimerTask(long delay, Object ref){
    if(tcpTimer == null)
      tcpTimer = new Timer(false);
    return new TCPTimerTask(tcpTimer, delay, this, ref);
  }


  /**
   * handle timer expiration (called by TCPTimerTask)
   * @param ref Generic reference that can be used by the timer to return 
   * information.
   */
  public synchronized void handleTimer(Object ref) {
      if (state == State.CLOSED) return;
    
    if (ref.equals("TIME_WAIT")) {
        changeState(State.CLOSED);
        notifyAll();
        tcpTimer.cancel();
        tcpTimer = null;
    } else {
        // resend the dropped packet
        TCPPacket packet = (TCPPacket) ref;
        TCPWrapper.send(packet, remoteAddress);
        createTimerTask(2000, packet);  // keep retrying
    }
}
}
