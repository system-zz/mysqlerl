/*
 * MySQL port driver.
 *
 * Copyright (C) 2008, Brian Cully <bjc@kublai.com>
 */

#include <mysql.h>

#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

const char *LOGPATH = "/tmp/mysqlerl.log";
const size_t BUFSIZE = 2048;
static FILE *logfile = NULL;

typedef u_int32_t msglen_t;

void
openlog()
{
  logfile = fopen(LOGPATH, "a");
}

void
closelog()
{
  fclose(logfile);
}

void
logmsg(const char *format, ...)
{
  FILE *out = logfile;
  va_list args;

  if (logfile == NULL)
    out = stderr;

  va_start(args, format);
  (void)vfprintf(out, format, args);
  (void)fprintf(out, "\n");
  va_end(args);

  fflush(out);
}

int
restartable_read(char *buf, size_t buflen)
{
  ssize_t rc, readb;

  rc = 0;
  READLOOP:
  while (rc < buflen) {
    readb = read(STDIN_FILENO, buf + rc, buflen - rc);
    if (readb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto READLOOP;

      return -1;
    } else if (readb == 0) {
      logmsg("ERROR: EOF trying to read additional %d bytes from "
             "standard input", buflen - rc);
      return -1;
    }

    rc += readb;
  }

  return rc;
}

int
restartable_write(const char *buf, size_t buflen)
{
  ssize_t rc, wroteb;

  rc = 0;
  WRITELOOP:
  while (rc < buflen) {
    wroteb = write(STDOUT_FILENO, buf + rc, buflen - rc);
    if (wroteb == -1) {
      if (errno == EAGAIN || errno == EINTR)
        goto WRITELOOP;

      return -1;
    }

    rc += wroteb;
  }

  return rc;
}

char *
read_cmd()
{
  char *buf;
  msglen_t len;

  logmsg("DEBUG: reading message length.");
  if (restartable_read((char *)&len, sizeof(len)) == -1) {
    logmsg("ERROR: couldn't read %d byte message prefix: %s.",
           sizeof(len), strerror(errno));
    exit(2);
  }
  len = ntohl(len);

  buf = malloc(len);
  if (buf == NULL) {
    logmsg("ERROR: Couldn't malloc %d bytes: %s.", len,
           strerror(errno));
    exit(2);
  }
  memset(buf, 0, BUFSIZE);

  logmsg("DEBUG: reading message body (len: %d).", len);
  if (restartable_read(buf, len) == -1) {
    logmsg("ERROR: couldn't read %d byte message: %s.",
           len, strerror(errno));
    exit(2);
  }

  return buf;
}

int
write_cmd(const char *cmd, msglen_t len)
{
  msglen_t nlen;

  nlen = htonl(len + 3);
  if (restartable_write((char *)&nlen, sizeof(nlen)) == -1)
    return -1;
  if (restartable_write(" - ", 3) == -1)
    return -1;
  if (restartable_write(cmd, len) == -1)
    return -1;

  return 0;
}

void
dispatch_db_cmd(MYSQL *dbh, const char *cmd)
{
  logmsg("DEBUG: dispatch_cmd(\"%s\")", cmd);
  write_cmd(cmd, strlen(cmd));
}

void
usage()
{
  fprintf(stderr, "Usage: mysqlerl host port db_name user passwd\n");
  exit(1);
}

int
main(int argc, char *argv[])
{
  MYSQL dbh;
  char *host, *port, *db_name, *user, *passwd, *cmd;

  openlog();
  logmsg("INFO: starting up.");

  if (argc < 6)
    usage();

  host    = argv[1];
  port    = argv[2];
  db_name = argv[3];
  user    = argv[4];
  passwd  = argv[5];

  mysql_init(&dbh);
  if (mysql_real_connect(&dbh, host, user, passwd,
                         db_name, atoi(port), NULL, 0) == NULL) {
    logmsg("ERROR: Failed to connect to database %s: %s (%s:%s).",
           db_name, mysql_error(&dbh), user, passwd);
    exit(2);
  }

  while ((cmd = read_cmd()) != NULL) {
    dispatch_db_cmd(&dbh, cmd);
    free(cmd);
  }

  mysql_close(&dbh);

  logmsg("INFO: shutting down.");
  closelog();

  return 0;
}
