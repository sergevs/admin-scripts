package main

// #include <unistd.h>
import "C"

import (
  "os"
  "path"
  "os/exec"
  "flag"
  "fmt"
  "bytes"
  "io"
  "bufio"
  "log"
  "strings"
  "time"
)

var Usage = func() {
        pName := path.Base( os.Args[0] )
        fmt.Fprintf( os.Stderr, "\nUsage of %s:\n\n", pName )
        fmt.Fprintf( os.Stderr, "%s [options] <configuration file> <command>\n", pName )
        fmt.Fprintf( os.Stderr, "  OR\n" )
        fmt.Fprintf( os.Stderr, "<commands from stdin> | %s [options]\n\n", pName )
        fmt.Fprintf( os.Stderr, "options:\n" )
        flag.PrintDefaults()
        fmt.Fprintf( os.Stderr, "\n" )
}

//var opt_H = flag.String( "H", "", "list of remote hosts to execute command #NOT IMPLEMENTED YET" )
//var opt_v = flag.Bool( "v", true, "print output of the commands in multithread mode #NOT IMPLEMENTED YET" )
var opt_s = flag.Bool( "s", false, "show configuration and commands without command execution" )
var opt_w = flag.Int( "w", 128, "maximum number of paralel workers processes. if the option value <=1, running commands in single process interactive mode" )
var opt_t = flag.Int( "t", 60, "command timeout in seconds" )
var opt_c = flag.Bool( "c", false, "force color output even if a terminal is not detected on stdout" )
var opt_p = flag.Bool( "p", false, "disable pager" )

type cfg struct {
  iterList []string
  options string
  cmd string
}

func ( c *cfg ) parseConfig( cfgFileName string ) error {
  fi, err := os.Stat( cfgFileName )
// see if the file is accessible or find it in path
  if err != nil || ! fi.Mode().IsRegular() {
    p := strings.Split(os.Getenv("PATH"), ":" )
    for i := 0; i<len( p ); i++ {
      fi , err := os.Stat( p[i] + "/" + cfgFileName )
      if err == nil && fi.Mode().IsRegular(){
        cfgFileName = p[i] + "/" + cfgFileName
        break
      }
    }
  }
  f, err  := os.Open( cfgFileName )
  if  err != nil {
    f.Close()
    log.Fatal( fmt.Errorf( "Configuration file %s is not found in path", cfgFileName ))
  }
  var scanner *bufio.Scanner
  fh := make( []byte,4 )
  f.Read( fh )

  if string( fh ) == "#ini" {
    scanner = bufio.NewScanner( f )
  } else {
    f.Close()
    cfgOut, cfgErr := exec.Command( "sh", "-c", fmt.Sprintf(" . %s && (echo host_list=$host_list; echo iter_list=$iter_list; echo user=$user; echo ssh_options=$ssh_options; echo cmd_options=$cmd_options)", cfgFileName )).CombinedOutput()
    if cfgErr != nil {
      fmt.Printf( "%s", cfgOut )
      log.Fatal( cfgErr )
    }
    scanner = bufio.NewScanner( bytes.NewBuffer( cfgOut ))
  }

  for scanner.Scan() {
    cfgOpt := strings.TrimSpace( scanner.Text())
    if len( cfgOpt ) > 0 && cfgOpt[0] != '#' {
      p := strings.SplitN( cfgOpt, "=", 2 )
      if len( p ) > 1 {
        key := strings.TrimSpace( p[0] )
        val := strings.TrimSpace( p[1] )
        if len( val ) > 0 {
          switch  key {
            case "host_list", "iter_list" :
              c.iterList = strings.Fields( val )
            case "ssh_options","cmd_options":
              c.options += val + " "
            case "user":
              c.options += "-oUser=" + strings.Trim( val , "@" ) + " "
            case "cmd":
              c.cmd = val
          }
        }
      }
    }
  }
  if c.cmd == "" {
    c.cmd = "ssh"
  }
  return nil
}

func ( c cfg ) String() string {
  return fmt.Sprintf( "cmd = %s %s\niterList = %s", c.cmd, c.options, strings.Join( c.iterList, " " ))
}

const C_RED   = "\x1b[31m"
const C_GREEN = "\x1b[32m"
const C_CYAN  = "\x1b[36m"
const C_DEF   = "\x1b[m"

type Reader interface {
   Read(p []byte) (n int, err error)
}

type colorStream struct {
  reader Reader
  color string //prefix lines string
  start bool   //indicate start stream
}

func NewColorStream ( r Reader ) *colorStream {
  return &colorStream{r, C_RED, true}
}

func ( c *colorStream ) Read( p []byte ) ( n int, err error ) {

  if c.start && len(c.color) > 0 {
    nr := copy( p, c.color )
    c.start = false
    return nr, nil
  }

  rBuffer := make ( []byte, len(p) )
  nr, er := c.reader.Read( rBuffer )
  if nr > 0 {
    copy( p, rBuffer )
    return nr, er
  } else {
    if er == io.EOF && len(c.color) > 0 {
      nr = copy ( p, []byte(C_DEF) )
      return nr, er
    }
    return nr, er
  }
}

type logger struct {
  level int
  color bool
  oStream io.WriteCloser
}

func NewLogger ( clevel int ) *logger {
    return &logger{clevel, int( C.isatty( C.int( os.Stdout.Fd() ))) != 0, os.Stdout}
}

func ( l logger ) printOut ( clevel int, format string, a ...interface{} ) {
  if clevel >= l.level {
    if l.color {
      fmt.Fprintf( l.oStream, C_CYAN )
    }
    fmt.Fprintf( l.oStream, format, a... )
    if l.color {
      fmt.Fprintf( l.oStream, C_DEF )
    }
  }
}

func ( l logger ) printState ( iterItem string, processState string ) {
  if l.level >= 1 {
    var  pState string
    if l.color {
      if  processState == "exit status 0" {
         pState = C_GREEN +  processState + C_DEF
      } else {
         pState = C_RED +  processState + C_DEF
      }
    } else {
      pState = processState
    }
    if l.color {
      fmt.Fprintf( l.oStream, C_CYAN + "=== [ %s ] %s" + C_CYAN + " ===" + C_DEF +"\n", iterItem, pState )
    } else {
      fmt.Fprintf( l.oStream, "=== [ %s ] %s ===\n", iterItem, pState )
    }
  }
}

type wStatus struct {
  n int
  err error
}

func main() {
  var conf  cfg
  var pCmd  []string
  var cPipe bool
  var iterList *[]string
  var oPager *exec.Cmd

  flag.Parse()
  pOut := NewLogger(1)

  if *opt_c {
    pOut.color = true
  }

  // detect and process stdin
  fi, _ := os.Stdin.Stat()
  if fi.Mode() & os.ModeNamedPipe == 0 {
    if len ( flag.Args() ) == 0 {
      Usage()
      log.Fatal( fmt.Errorf( "ERROR: <configuration> <command> arguments or command pipe from stdin is required" ))
    }
    cPipe = false
    conf.parseConfig( flag.Arg(0) )
    iterList = &conf.iterList
  } else {
    if len ( flag.Args()) != 0 { // || len( *opt_H ) != 0 {
      Usage()
      log.Fatal( fmt.Errorf( "ERROR: Additional arguments or -H option are not allowed with command pipe from stdin" ))
    }
    scanner := bufio.NewScanner( os.Stdin )
    for scanner.Scan() {
      cl := strings.TrimSpace( scanner.Text())
      if len( cl ) > 0 {
        pCmd = append( pCmd, cl )
      }
    }
    iterList = &pCmd
    cPipe = true
  }

  // dump configuration
  if *opt_s {
    if cPipe {
      fmt.Printf( "iterList = %s\n", strings.Join( pCmd, ", " ))
    } else {
      fmt.Println( conf )
    }
    os.Exit( 0 )
  }

  // single process mode ( as per Max request :) )

  if *opt_w <= 1 {
    var wcmd *exec.Cmd
    pOut.printOut( 1, "= Single process interactive mode, pager is switched off =\n" )

    for i := 0; i<len(*iterList); i++ {
      if cPipe {
          wcmd = exec.Command( "sh", "-c", fmt.Sprintf( "\"%s\"", (*iterList)[i] ))
      } else {
          wcmd = exec.Command( "sh", "-c", fmt.Sprintf("%s %s %s \"%s\"", conf.cmd, conf.options, (*iterList)[i], strings.Join(flag.Args()[1:], " ")))
      }
      wcmd.Stdin = os.Stdin
      wcmd.Stdout = os.Stdout
      wcmd.Stderr = os.Stderr
      pOut.printState( (*iterList)[i], "" )
      timer := time.AfterFunc( time.Duration( *opt_t ) *time.Second, func () {
          if err := wcmd.Process.Kill(); err != nil {
            log.Fatal( err ) // that should never be happened - can't kill a process.
          }
      })
      if err := wcmd.Run(); err != nil { pOut.printState( (*iterList)[i], err.Error() ) }
      timer.Stop()
    }
    os.Exit(0)
  }

  // pager
  if ! *opt_p {
    oPager = exec.Command("less", "-rFX")
    oPager.Stdout = os.Stdout
    oPager.Stderr = os.Stderr
    var err error
    if pOut.oStream, err  = oPager.StdinPipe(); err != nil { log.Fatal( err ) }
    if err = oPager.Start(); err != nil { log.Fatal( err ) }
  }

  // main exec loop(s)
  iWorkers := 1
  jWorkers := 1

  if len( *iterList ) <= *opt_w {
    iWorkers = len( *iterList )
  } else {
    iWorkers = *opt_w
    jWorkers = int( len( *iterList )/(*opt_w))
    if iWorkers * jWorkers < len( *iterList ) {
      jWorkers ++
    }
  }

  pOut.printOut( 1, "= Entering into Exec section, workers=%d, iterations=%d, timeout=%d =\n", iWorkers, jWorkers, *opt_t )

  wcmd := make( []*exec.Cmd ,iWorkers )
  out  := make( []bytes.Buffer, iWorkers )
  oer  := make( []bytes.Buffer, iWorkers )
  done := make( chan wStatus, iWorkers )

  for j := 0; j<jWorkers; j++ {
    if jWorkers > 1 {
      pOut.printOut( 1, "== Iteration=%d ", j+1 )
    } else {
      pOut.printOut( 1, "== ")
    }
    if (j+1)*iWorkers > len( *iterList ) {
      iWorkers = len( *iterList ) -  ( j*iWorkers )
    }
    cIdx := j*(*opt_w)

    for i := 0; i<iWorkers; i++ {
      pOut.printOut( 1, "[ %s ]", (*iterList)[cIdx+i] )
      if cPipe {
        wcmd[i] = exec.Command( "sh", "-c", fmt.Sprintf( "\"%s\"", (*iterList)[cIdx+i] ))
      } else {
        wcmd[i] = exec.Command( "sh", "-c", fmt.Sprintf("%s %s %s \"%s\"", conf.cmd, conf.options, (*iterList)[cIdx+i], strings.Join(flag.Args()[1:], " ")))
//        fmt.Printf("%s %s %s \"%s\"\n", conf.cmd, conf.options, (*iterList)[cIdx+i], strings.Join(flag.Args()[1:], " "))
      }
      stdin, err := wcmd[i].StdinPipe()
      if err != nil { log.Fatal( err ) }
      stdin.Close()
      stdout, err := wcmd[i].StdoutPipe()
      if err != nil { log.Fatal( err ) }
      stderr, err := wcmd[i].StderrPipe()
      if err != nil { log.Fatal( err ) }

      if err := wcmd[i].Start(); err != nil { log.Fatal( err ) }

      go func ( n int ) { out[n].ReadFrom( stdout ) }( i )
      go func ( n int ) { oer[n].ReadFrom( stderr ) }( i )

// creating non blocking wait, must pass i as argument to avoid unexpected results
// function closure is required to run time.AfterFunc in the current iteration context
      func ( n int ) {
        timer := time.AfterFunc( time.Duration( *opt_t ) *time.Second, func () {
          if err := wcmd[n].Process.Kill(); err != nil {
            log.Fatal( err ) // that should never be happened - can't kill a process.
          }
        })
        go func () {
          done <- wStatus{ n, wcmd[n].Wait() }
          timer.Stop()
        }()
      }( i )
    }
    pOut.printOut( 1, " ==\n")

    for i := 0; i<iWorkers; i++ {
      job := <- done
      pOut.printState( (*iterList)[cIdx +job.n], wcmd[job.n].ProcessState.String() )
      io.Copy( pOut.oStream, &out[job.n] )
      if ! *opt_p {
        io.Copy( pOut.oStream, NewColorStream(&oer[job.n]) )
      } else {
        if pOut.color {
          io.Copy( os.Stderr, NewColorStream(&oer[job.n]) )
        } else {
          io.Copy( os.Stderr, &oer[job.n] )
        }
      }
    }
  }

  if ! *opt_p {
    pOut.oStream.Close()
    oPager.Wait()
  }
}
