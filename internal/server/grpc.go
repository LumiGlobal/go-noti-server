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
	var (
		port     = os.Getenv("PORT")
		lis, err = net.Listen("tcp", port)
		s        = grpc.NewServer(grpc.ChainUnaryInterceptor(nrgrpc.UnaryServerInterceptor(telemetry.App), AuthInterceptor))
	)

	pb.RegisterNotificationServiceServer(s, &server{})
	pbh.RegisterHealthServiceServer(s, &healthCheckServer{})

	telemetry.Log(zerolog.InfoLevel, fmt.Sprintf("server listening at %v", lis.Addr()))

	if err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to listen: %v", err))
	}

	if err := s.Serve(lis); err != nil {
		telemetry.Log(zerolog.FatalLevel, fmt.Sprintf("failed to serve: %v", err))
	}
}

func (s *server) SendMessage(ctx context.Context, req *pb.NotificationRequest) (*pb.NotificationResponse, error) {
	start := time.Now()

	txn := newrelic.FromContext(ctx)
	defer txn.End()

	jobId := txn.GetTraceMetadata().TraceID
	traceHeaders := http.Header{}
	txn.InsertDistributedTraceHeaders(traceHeaders)
	telemetry.AddTraceHeaders(jobId, traceHeaders)
	logger := telemetry.NewLogger(ctx)

	notification := req.GetNotification()
	log(logger, zerolog.InfoLevel, notification, jobId, "Notification request received")
	defer func() {
		msg := fmt.Sprintf("Notification response returned. SendMessage call duration: %v", time.Since(start))
		log(logger, zerolog.InfoLevel, notification, jobId, msg)
	}()

	seg := txn.StartSegment("MarshallingNotification")
	marshaller := proto.MarshalOptions{Deterministic: true}
	data, err := marshaller.Marshal(notification)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("error marshalling notification: %v", err)
		log(logger, zerolog.ErrorLevel, notification, jobId, msg)
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
		log(logger, zerolog.ErrorLevel, notification, jobId, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}
	if !unique {
		msg := "notification payload not unique"
		txn.NoticeError(fmt.Errorf(msg))
		log(logger, zerolog.WarnLevel, notification, jobId, msg)
		return nil, status.Errorf(codes.AlreadyExists, msg)
	}

	err = datastore.SetJobIdToPayload(ctx, jobId, data)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("ERROR SETTING KEY %v TO PAYLOAD: %v", jobId, err)
		log(logger, zerolog.ErrorLevel, notification, jobId, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	err = datastore.PushJobIdToJobsQueue(ctx, jobId)
	if err != nil {
		txn.NoticeError(err)
		msg := fmt.Sprintf("ERROR ADDING %v TO JOBS QUEUE: %v", jobId, err)
		log(logger, zerolog.ErrorLevel, notification, jobId, msg)
		return nil, status.Errorf(codes.Internal, msg)
	}

	return &pb.NotificationResponse{Message: "Message Received"}, nil
}

func (s *healthCheckServer) Check(ctx context.Context, req *pbh.HealthCheckRequest) (*pbh.HealthCheckResponse, error) {
	return &pbh.HealthCheckResponse{Message: "Alive"}, nil
}

func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {

	token := extractFromContext(ctx)
	if !isTokenValid(token) {
		telemetry.LogWithContext(zerolog.WarnLevel, "invalid auth token", ctx)
		return nil, status.Errorf(codes.Unauthenticated, "token invalid")
	}
	return handler(ctx, req)
}

func log(logger zerolog.Logger, level zerolog.Level, notification *pb.NotificationPackage, jobId string, msg string) {
	logger.WithLevel(level).
		Str("title", notification.Title).
		Str("body", notification.Body).
		Str("path", notification.Data["path"]).
		Str("contentId", notification.Data["contentId"]).
		Str("contentType", notification.Data["contentType"]).
		Str("jobId", jobId).
		Msg(fmt.Sprintf("[SendMessage] [%v] %v", jobId, msg))
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
