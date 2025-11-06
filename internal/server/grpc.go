package server

import (
	"context"
	"fmt"
	"go-noti-server/internal/datastore"
	"go-noti-server/internal/telemetry"
	pbh "go-noti-server/protos/health"
	pb "go-noti-server/protos/notifications"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/newrelic/go-agent/v3/integrations/nrgrpc"
	"github.com/newrelic/go-agent/v3/newrelic"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type server struct {
	pb.UnimplementedNotificationServiceServer
}

type healthCheckServer struct {
	pbh.UnimplementedHealthServiceServer
}

func RunGrpcServer() {
	port := os.Getenv("PORT")
	lis, err := net.Listen("tcp", port)
	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to listen: %v", err))
	}

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(nrgrpc.UnaryServerInterceptor(telemetry.App), AuthInterceptor))

	pb.RegisterNotificationServiceServer(s, &server{})
	pbh.RegisterHealthServiceServer(s, &healthCheckServer{})

	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("server listening at %v", lis.Addr()))

	err = s.Serve(lis)
	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to serve: %v", err))
	}
}

func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	token := extractFromContext(ctx)
	if !isTokenValid(token) {
		telemetry.LogWithContext(zerolog.WarnLevel, "invalid auth token", ctx)
		return nil, status.Errorf(codes.Unauthenticated, "token invalid")
	}
	return handler(ctx, req)
}

func extractFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	tokens := md.Get("Authorization")
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func isTokenValid(token string) bool {
	authed := os.Getenv("AUTHED")
	return token == authed
}

func (s *healthCheckServer) Check(ctx context.Context, req *pbh.HealthCheckRequest) (*pbh.HealthCheckResponse, error) {
	return &pbh.HealthCheckResponse{Message: "Alive"}, nil
}

func (s *server) SendMessage(ctx context.Context, req *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	start := time.Now()

	txn := newrelic.FromContext(ctx)
	defer txn.End()
	jobId := txn.GetTraceMetadata().TraceID
	distributeTracing(txn, jobId)

	notification := req.GetNotification()

	logger := newGrpcLogger(ctx, notification, jobId)
	logger.log(zerolog.InfoLevel, "Notification request received")
	defer func() {
		logger.log(zerolog.InfoLevel, fmt.Sprintf("Notification response returned. SendMessage call duration: %v", time.Since(start)))
	}()

	seg := txn.StartSegment("MarshallingNotification")
	marshaller := proto.MarshalOptions{Deterministic: true}
	data, err := marshaller.Marshal(notification)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("error marshalling notification: %v", err)
		logger.log(zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	seg.End()

	seg = txn.StartSegment("HashingNotification")
	hash := xxhash.Sum64(data)
	seg.End()

	unique, err := datastore.AddJobPayloadHashToSet(ctx, hash)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("ERROR ADDING %v TO SET: %v", hash, err)
		logger.log(zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if !unique {
		msg := "notification payload not unique"
		txn.NoticeError(fmt.Errorf(msg))
		logger.log(zerolog.WarnLevel, msg)
		return nil, status.Errorf(codes.AlreadyExists, msg)
	}

	err = datastore.SetJobIdToPayload(ctx, jobId, data)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("ERROR SETTING KEY %v TO PAYLOAD: %v", jobId, err)
		logger.log(zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	err = datastore.PushJobIdToJobsQueue(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("ERROR ADDING %v TO JOBS QUEUE: %v", jobId, err)
		logger.log(zerolog.ErrorLevel, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	return &pb.NotificationResponse{Message: "Message Received"}, nil
}

func distributeTracing(txn *newrelic.Transaction, jobId string) {
	traceHeaders := http.Header{}
	txn.InsertDistributedTraceHeaders(traceHeaders)
	telemetry.AddTraceHeaders(jobId, traceHeaders)
}

type grpcLogger struct {
	logger       zerolog.Logger
	notification *pb.NotificationPackage
	jobId        string
}

func newGrpcLogger(ctx context.Context, notification *pb.NotificationPackage, jobId string) grpcLogger {
	return grpcLogger{
		logger:       telemetry.NewLogger(ctx),
		notification: notification,
		jobId:        jobId,
	}
}

func (gl *grpcLogger) log(level zerolog.Level, msg string) {
	gl.logger.WithLevel(level).
		Str("title", gl.notification.Title).
		Str("body", gl.notification.Body).
		Str("path", gl.notification.Data["path"]).
		Str("contentId", gl.notification.Data["contentId"]).
		Str("contentType", gl.notification.Data["contentType"]).
		Str("jobId", gl.jobId).
		Msg(fmt.Sprintf("[SendMessage] [%v] %v", gl.jobId, msg))
}
